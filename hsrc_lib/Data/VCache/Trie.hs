{-# LANGUAGE BangPatterns, DeriveDataTypeable #-}
-- | A compact bytestring trie implemented above VCache.
module Data.VCache.Trie
    ( Trie
    , trie_space
    , empty, singleton
    , null, size
    , addPrefix

    , unionWith

    , toList
    , toListBy
    , foldr, foldr', foldrWithKey, foldrWithKey'
    , foldl, foldl', foldlWithKey, foldlWithKey'

    , map, mapWithKey
    ) where

import Control.Applicative
import Control.Monad
import Data.Bits
import Data.Word
import Data.ByteString (ByteString)
import qualified Data.ByteString as B
import qualified Data.ByteString.Builder as BB
import qualified Data.ByteString.Internal as BSI
import Data.Typeable
import qualified Data.Array.IArray as A
import qualified Data.List as L
import Data.Maybe

import Foreign.ForeignPtr
import Foreign.Ptr
import Foreign.Storable

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
type Child a = Maybe (VRef (Trie a))

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
    get = Trie <$> get
    put = put . trie_root

instance (Show a) => Show (Trie a) where
    showsPrec p t = showParen (p > 0) $
        "Data.VCache.Trie.fromList " ++ shows (toList t)

-- | Construct Trie with no elements.
empty :: VSpace -> Trie a
empty = Trie Nothing
{-# INLINE empty #-}

emptyChildren :: Children a
emptyChildren = A.accumArray const Nothing (minBound, maxBound) [] 

-- | Construct Trie with one element.
singleton :: (VCacheable a) => VSpace -> a -> Trie a
singleton a !vc = Trie (Just pRoot) vc where
    pRoot = vref vc node
    node = Node emptyChildren B.empty (Just a)

-- | O(1), test whether trie is empty.
null :: Trie a -> Bool
null t = isNothing . trie_root
{-# INLINE null #-}

-- | O(n), compute size of the trie.
size :: Trie a -> Int
size t = childSize . trie_root
{-# INLINE size #-}

childSize :: Child a -> Int
childSize = maybe 0 (nodeSize . deref')

nodeSize :: Node a -> Int
nodeSize (Node c _ v) =
    let csz = L.foldl' (+) 0 $ fmap childSize (A.elems c) in
    let vsz = if isNothing v then 0 else 1 in
    vsz + csz

-- | Obtain a list of key, value pairs from a trie.
toList :: Trie a -> [(ByteString, a)]
toList = toListBy (,)
{-# INLINE toList #-}

elems :: Trie a -> [a]
elems = toListBy (\ _ v -> v)
{-# INLINE elems #-}

keys :: Trie a -> [ByteString]
keys = toListBy (\ k _ -> k)
{-# INLINE keys #-}

toListBy :: (ByteString -> a -> b) -> Trie a -> [b]
toListBy fn = foldrWithKey (\ k v bs -> fn k v : bs) []
{-# INLINE toListBy #-}

-- | add a common prefix to all existing keys in a Trie. This may be
-- used to model mounting into a directory structure, for example.
addPrefix :: (VCacheable a) => ByteString -> Trie a -> Trie a
addPrefix _ t@(Trie Nothing _) = t
addPrefix p (Trie (Just pRoot) vc) =
    let root = deref' pRoot in
    let p' = p `B.append` (trie_prefix root) in
    let root' = root { trie_prefix = p' } in
    let pRoot' = vref vc root' in
    Trie (Just $! pRoot') vc

-- should probably have some variations to:
--   extract a whole sub-trie by prefix
--   delete a whole sub-trie by prefix


foldrWithKey, foldrWithKey' :: (ByteString -> a -> b -> b) -> b -> Trie a -> b
foldlWithKey, foldlWithKey' :: (b -> ByteString -> a -> b) -> b -> Trie a -> b
foldrWithKey fn b0  = _foldrWithKey  fn b0 . trie_root
foldrWithKey' fn b0 = _foldrWithKey' fn b0 . trie_root
foldlWithKey fn b0  = _foldlWithKey  fn b0 . trie_root
foldlWithKey' fn b0 = _foldlWithKey' fn b0 . trie_root
{-# INLINE foldrWithKey #-}
{-# INLINE foldrWithKey' #-}
{-# INLINE foldlWithKey #-}
{-# INLINE foldlWithKey' #-}

foldr, foldr' :: (a -> b -> b) -> b -> Trie a -> b
foldl, foldl' :: (b -> a -> b) -> b -> Trie a -> b
foldr  = foldrWithKey  . const
foldr' = foldrWithKey' . const
foldl  = foldlWithKey  . const
foldl' = foldlWithKey' . const
{-# INLINE foldr #-}
{-# INLINE foldr' #-}
{-# INLINE foldl #-}
{-# INLINE foldl' #-}


_foldrWithKey, _foldrWithKey' :: (ByteString -> a -> b -> b) -> b -> Child a -> b
_foldlWithKey, _foldlWithKey' :: (b -> ByteString -> a -> b) -> b -> Child a -> b



fold







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
