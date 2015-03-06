{-# LANGUAGE BangPatterns, PatternGuards, DeriveDataTypeable #-}
-- | A compact bytestring trie implemented above VCache.
module Data.VCache.Trie
    ( Trie
    , trie_space
    , empty, singleton
    , null, size
    , addPrefix
    , lookup, lookup', lookupc
    , lookupPrefix

    , toList, toListBy, elems, keys
    , foldr, foldr', foldrM, foldrWithKey, foldrWithKey', foldrWithKeyM
    , foldl, foldl', foldlM, foldlWithKey, foldlWithKey', foldlWithKeyM

    -- , map, mapM, mapWithKey, mapWithKeyM 

    ) where

import Prelude hiding (null, lookup, foldr, foldl)
import Control.Applicative hiding (empty)
import Control.Monad
import Control.Exception (assert)
import Data.Bits
import Data.Word
import Data.ByteString (ByteString)
import qualified Data.ByteString as B
import qualified Data.ByteString.Lazy as LB
import qualified Data.ByteString.Builder as BB
import qualified Data.ByteString.Internal as BSI
import Data.Typeable
import qualified Data.Array.IArray as A
import qualified Data.List as L
import Data.Maybe
import Data.Monoid

import Foreign.ForeignPtr
import Foreign.Ptr
import Foreign.Storable

import Strict
import Ident

import Database.VCache

-- Thoughts:
--
-- I think we shouldn't encode too much information into any one
-- node, so I'm aiming for:
--
--   encode at most one extended prefix per node
--   encode at most one value per node
--   encode at most 256 references to child nodes
-- 
-- The high branching factor allows for a relatively sparse trie.
-- But it's simple and in many cases should help avoid deep search
-- or update (which is more expensive in VCache than in memory).
--
-- This encoding doesn't make the invariants of a trie especially
-- easy to reason about. Fortunately, tries are relatively easy to
-- reason about even with a simplistic encoding.
--

data Node a = Node
    { trie_branch :: {-# UNPACK #-} !(Children a) -- arity 256; one byte from prefix
    , trie_prefix :: {-# UNPACK #-} !ByteString   -- compact extended prefix
    , trie_accept :: !(Maybe a)                   -- value at specific node
    } deriving (Eq, Typeable)
-- Invariant for nodes: either we branch or we accept.

type Children a = A.Array Word8 (Child a)
type Child a = Maybe (VRef (Node a))

-- | The Trie data structure. 
--
-- The element type for the trie will be stored directly within each
-- node. So, if it's very large and shouldn't be parsed during lookup,
-- developers should use a VRefs in the data type. For small data, it
-- is likely better for performance to pack the data into each node.
-- 
-- A trie with content is serialized as a pointer to the root node.
--
data Trie a = Trie 
    { trie_root  :: !(Child a)
    , trie_space :: !VSpace
    } deriving (Eq, Typeable) 

instance (VCacheable a) => VCacheable (Node a) where
    get = Node <$> getChildren <*> get <*> get
    put (Node c p v) = putChildren c >> put p >> put v
instance (VCacheable a) => VCacheable (Trie a) where
    get = Trie <$> get <*> getVSpace
    put = put . trie_root

instance (Show a) => Show (Trie a) where
    showsPrec p t = showParen (p > 0) $
        showString "Data.VCache.Trie.fromList " .
        shows (toList t)

-- | Construct Trie with no elements.
empty :: VSpace -> Trie a
empty = Trie Nothing
{-# INLINE empty #-}

emptyChildren :: Children a
emptyChildren = A.accumArray const Nothing (minBound, maxBound) [] 

-- | Construct Trie with one element.
singleton :: (VCacheable a) => VSpace -> a -> Trie a
singleton vc a = Trie (Just pRoot) vc where
    pRoot = vref vc node
    node = Node emptyChildren B.empty (Just a)

-- | O(1), test whether trie is empty.
null :: Trie a -> Bool
null = isNothing . trie_root
{-# INLINE null #-}

-- | O(n). Compute size of the trie.
size :: Trie a -> Int
size = foldr' (const (1 +)) 0 
{-# INLINE size #-}

-- | O(n). Obtain a list of (key,val) pairs, sorted by key.
toList :: Trie a -> [(ByteString, a)]
toList = toListBy (,)
{-# INLINE toList #-}

-- | O(n). Obtain list of elements in the trie.
elems :: Trie a -> [a]
elems = toListBy (flip const)
{-# INLINE elems #-}

-- | O(n). Obtain a sorted list of of keys.
keys :: Trie a -> [ByteString]
keys = toListBy const
{-# INLINE keys #-}

toListBy :: (ByteString -> a -> b) -> Trie a -> [b]
toListBy fn = foldrWithKey (\ k v bs -> fn k v : bs) []
{-# INLINE toListBy #-}

-- | O(1). Add a common prefix to all keys currently in the Trie
addPrefix :: (VCacheable a) => ByteString -> Trie a -> Trie a
addPrefix _ t@(Trie Nothing _) = t
addPrefix prefix (Trie (Just pRoot) vc) =
    let root = deref' pRoot in
    let p' = prefix `B.append` (trie_prefix root) in
    let root' = root { trie_prefix = p' } in
    Trie (Just $! vref vc root') vc

-- | Lookup an object by key without caching nodes
lookup' :: ByteString -> Trie a -> Maybe a
lookup' k = _lookup deref' k . trie_root

-- | Lookup an object by key
lookup :: ByteString -> Trie a -> Maybe a
lookup = lookupc CacheMode1

-- | Lookup object by key with a specific cache mode.
lookupc :: CacheMode -> ByteString -> Trie a -> Maybe a
lookupc cm k = _lookup (derefc cm) k . trie_root

_lookup :: (VRef (Node a) -> Node a) -> ByteString -> Child a -> Maybe a
_lookup _ _ Nothing = Nothing
_lookup d key (Just c) =
    let tn = d c in
    let pre = trie_prefix tn in
    let s = sharedPrefixLen key pre in
    let k = B.length key in
    let p = B.length pre in
    if (s < p) then Nothing else -- couldn't match full prefix
    assert ((s == p) && (k >= p)) $ 
    if (k == p) then trie_accept tn else -- match prefix exactly
    let key' = B.drop (p+1) key in
    let c' = (trie_branch tn) A.! (B.index key p) in
    _lookup d key' c'
    
-- | Obtain a trie rooted at a given prefix.
--
-- This operation may need to allocate a new node.
lookupPrefix :: (VCacheable a) => ByteString -> Trie a -> Trie a
lookupPrefix k tr =
    let child = _lookupP k (trie_root tr) in
    Trie child (trie_space tr)

_lookupP :: (VCacheable a) => ByteString -> Child a -> Child a
_lookupP key c | B.null key = c -- stop on exact node
_lookupP _ Nothing = Nothing -- 
_lookupP key (Just c) = 
    let tn = deref' c in
    let pre = trie_prefix tn in
    let s = sharedPrefixLen key pre in
    let k = B.length key in
    let p = B.length pre in
    if (k <= p) -- test whether key should fit within prefix
       then if (s < k) then Nothing else
            assert (s == k) $
            let pre' = B.drop k pre in -- trim key from prefix
            let tn' = tn { trie_prefix = pre' } in
            Just $! vref (vref_space c) tn' -- allocate new node
       else if (s < p) then Nothing else 
            assert (s == p) $
            let key' = B.drop (p+1) key in
            let c' = (trie_branch tn) A.! (B.index key p) in
            _lookupP key' c' -- recursive lookup



-- More variations: 
--  extract or delete a trie by prefix.


foldrWithKey, foldrWithKey' :: (ByteString -> a -> b -> b) -> b -> Trie a -> b
foldlWithKey, foldlWithKey' :: (b -> ByteString -> a -> b) -> b -> Trie a -> b
{-# INLINE foldrWithKey  #-}
{-# INLINE foldrWithKey' #-}
{-# INLINE foldlWithKey  #-}
{-# INLINE foldlWithKey' #-}
foldrWithKey  fn b t = runIdent  $ foldrWithKeyM (apwf fn) b t
foldrWithKey' fn b t = runStrict $ foldrWithKeyM (apwf fn) b t
foldlWithKey  fn b t = runIdent  $ foldlWithKeyM (apwf fn) b t
foldlWithKey' fn b t = runStrict $ foldlWithKeyM (apwf fn) b t

apwf :: (Applicative f) => (a -> b -> c -> d) -> (a -> b -> c -> f d)
apwf fn a b c = pure (fn a b c)
{-# INLINE apwf #-}

foldr, foldr' :: (a -> b -> b) -> b -> Trie a -> b
foldl, foldl' :: (b -> a -> b) -> b -> Trie a -> b
foldrM :: Monad m => (a -> b -> m b) -> b -> Trie a -> m b
foldlM :: Monad m => (b -> a -> m b) -> b -> Trie a -> m b
{-# INLINE foldr  #-}
{-# INLINE foldr' #-}
{-# INLINE foldrM #-}
{-# INLINE foldl  #-}
{-# INLINE foldl' #-}
{-# INLINE foldlM #-}
foldr  = foldrWithKey  . skip1st
foldr' = foldrWithKey' . skip1st
foldrM = foldrWithKeyM . skip1st
foldl  = foldlWithKey  . skip2nd
foldl' = foldlWithKey' . skip2nd
foldlM = foldlWithKeyM . skip2nd

skip1st :: (b -> c) -> (a -> b -> c)
skip2nd :: (a -> c) -> (a -> b -> c)
{-# INLINE skip1st #-}
{-# INLINE skip2nd #-}
skip1st = const
skip2nd = flip . const

foldrWithKeyM :: (Monad m) => (ByteString -> a -> b -> m b) -> b -> Trie a -> m b
foldrWithKeyM ff = wr where
    wr b = wc mempty b . trie_root
    wc _ b Nothing = return b
    wc p b (Just c) =
        let tn = deref' c in
        let p' = nodePrefix p tn in -- extended prefix
        wlc p' (trie_branch tn) 255 b >>=
        maybe return (ff (toKey p')) (trie_accept tn)
    wlc p a !k b = 
        let cc = if (0 == k) then return else wlc p a (k-1) in
        let p' = p `mappend` BB.word8 k in -- branch prefix
        wc p' b (a A.! k) >>= cc
{-# NOINLINE foldrWithKeyM #-}

foldlWithKeyM :: (Monad m) => (b -> ByteString -> a -> m b) -> b -> Trie a -> m b
foldlWithKeyM ff = wr where
    wr b = wc mempty b . trie_root
    wc _ b Nothing = return b
    wc p b (Just c) =
        let tn = deref' c in
        let p' = nodePrefix p tn in -- extended prefix
        let cc = wlc p' (trie_branch tn) 0 in
        case trie_accept tn of
            Nothing -> cc b
            Just val -> ff b (toKey p') val >>= cc
    wlc p a !k b =
        let cc = if (255 == k) then return else wlc p a (k+1) in
        let p' = p `mappend` BB.word8 k in -- branch prefix
        wc p' b (a A.! k) >>= cc
{-# NOINLINE foldlWithKeyM #-}



toKey :: BB.Builder -> ByteString
toKey = LB.toStrict . BB.toLazyByteString      

nodePrefix ::  BB.Builder -> Node a -> BB.Builder
nodePrefix k tn = 
    let p = trie_prefix tn in
    if B.null p then k else k `mappend` BB.byteString p 






{-
unionWith :: (a ->
filterWithKey :: (ByteString -> a -> Bool) -> Trie a -> Trie a


map :: (VCacheable b) => (a -> b) -> Trie a -> Trie b
mapWithKey :: (VCacheable b) => (ByteString -> a -> b) -> Trie a -> Trie b



filterMap :: (VCacheable b) =
-}




getChildren :: (VCacheable a) => VGet (Children a)
getChildren = A.listArray (minBound, maxBound) <$> getChild256

-- obtain a list of exactly 256 elements
getChild256 :: (VCacheable a) => VGet [Child a]
getChild256 = L.concat <$> replicateM 32 getChild8

-- obtain a list of exactly 8 elements
getChild8 :: (VCacheable a) => VGet [Child a]
getChild8 = do
    f <- getWord8
    let rc n = if (0 == (f .&. (1 `shiftL` n))) 
                then return Nothing -- no child indicated 
                else Just <$> get -- pointer to child trie
    c0 <- rc 0
    c1 <- rc 1
    c2 <- rc 2
    c3 <- rc 3
    c4 <- rc 4
    c5 <- rc 5
    c6 <- rc 6
    c7 <- rc 7
    return [c0,c1,c2,c3
           ,c4,c5,c6,c7]

putChildren :: (VCacheable a) => Children a -> VPut ()
putChildren = putChildList . A.elems

putChildList :: (VCacheable a) => [Child a] -> VPut ()
putChildList lc = mapM_ put (bitFlags lc) >> mapM_ put (catMaybes lc)

bitFlags :: [Maybe a] -> [Word8]
bitFlags [] = []
bitFlags (c0:c1:c2:c3:c4:c5:c6:c7:cs) = f : bitFlags cs where
    f = (b c0 0) .|. 
        (b c1 1) .|.
        (b c2 2) .|.
        (b c3 3) .|.
        (b c4 4) .|.
        (b c5 5) .|.
        (b c6 6) .|.
        (b c7 7)
    b Nothing _ = 0
    b (Just _) n = 1 `shiftL` n
bitFlags _ = impossible "expecting multiple of 8 children"

impossible :: String -> a
impossible emsg = error $ "Data.VCache.Trie: " ++ emsg 





-- | Return byte count for prefix common among two strings.
sharedPrefixLen :: ByteString -> ByteString -> Int
sharedPrefixLen (BSI.PS s1 off1 len1) (BSI.PS s2 off2 len2) =
    BSI.inlinePerformIO $ 
    withForeignPtr s1 $ \ p1 ->
    withForeignPtr s2 $ \ p2 ->
    indexOfDiff (p1 `plusPtr` off1) (p2 `plusPtr` off2) (min len1 len2)

indexOfDiff :: Ptr Word8 -> Ptr Word8 -> Int -> IO Int
indexOfDiff !p1 !p2 !len = loop 0 where
    loop !idx = 
        if (idx == len) then return len else
        peekByte (p1 `plusPtr` idx) >>= \ c1 ->
        peekByte (p2 `plusPtr` idx) >>= \ c2 ->
        if (c1 /= c2) then return idx else
        loop (idx + 1)

-- an aide for type inference
peekByte :: Ptr Word8 -> IO Word8
peekByte = peek 
