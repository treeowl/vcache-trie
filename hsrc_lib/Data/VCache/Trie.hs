{-# LANGUAGE BangPatterns, PatternGuards, DeriveDataTypeable #-}
-- | A compact bytestring trie implemented above VCache.
module Data.VCache.Trie
    ( Trie
    , trie_space
    , empty, singleton
    , null, size
    , lookup, lookup', lookupc
    , prefixKeys, lookupPrefix, deletePrefix
    , insert, delete, adjust
    , insertList, deleteList

    , toList, toListBy, elems, keys
    , foldr, foldr', foldrM, foldrWithKey, foldrWithKey', foldrWithKeyM
    , foldl, foldl', foldlM, foldlWithKey, foldlWithKey', foldlWithKeyM

    , map, mapM, mapWithKey, mapWithKeyM 

    , validate
    , unsafeTrieAddr
    ) where

import Prelude hiding (null, lookup, foldr, foldl, map, mapM)
import Control.Applicative hiding (empty)
import qualified Control.Monad as M
import Control.Exception (assert)
import Data.Word
import Data.ByteString (ByteString)
import qualified Data.ByteString as B
import qualified Data.ByteString.Lazy as LB
import qualified Data.ByteString.Builder as BB
import qualified Data.ByteString.Internal as BSI
import qualified Data.Array.IArray as A
import qualified Data.List as L
import Data.Maybe
import Data.Monoid

import Foreign.ForeignPtr
import Foreign.Ptr
import Foreign.Storable

import Strict
import Ident
import Data.VCache.Trie.Type

import Database.VCache

-- | Construct Trie with no elements.
empty :: VSpace -> Trie a
empty = Trie Nothing
{-# INLINE empty #-}

-- | Construct Trie with one element.
singleton :: (VCacheable a) => VSpace -> ByteString -> a -> Trie a
singleton vc k a = Trie (Just $! vref vc (singletonNode k a)) vc

singletonNode :: ByteString -> a -> Node a
singletonNode k a = Node (mkChildren []) k (Just a)

mkChildren :: [(Word8, Child a)] -> Children a
mkChildren = A.accumArray (flip const) Nothing (minBound, maxBound)

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
prefixKeys :: (VCacheable a) => ByteString -> Trie a -> Trie a
prefixKeys _ t@(Trie Nothing _) = t
prefixKeys prefix (Trie (Just pRoot) vc) =
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

-- | Delete all keys sharing a given prefix. 
deletePrefix :: (VCacheable a) => ByteString -> Trie a -> Trie a
deletePrefix p (Trie c vc) = Trie c' vc where
    c' = _deleteP p c

_deleteP :: (VCacheable a) => ByteString -> Child a -> Child a 
_deleteP _ Nothing = Nothing
_deleteP key c@(Just pNode) = 
    let vc = vref_space pNode in
    let tn = deref' pNode in
    let pre = trie_prefix tn in
    let s = sharedPrefixLen key pre in
    let k = B.length key in
    let p = B.length pre in
    if (k <= p)
       then if (s < k) then c else -- no elements share prefix
            assert (s == k) $ Nothing -- all elements share prefix
       else if (s < p) then c else -- no elements share prefix
            assert (s == p) $
            let key' = B.drop (p+1) key in
            let idx  = B.index key p in
            let tgt  = (trie_branch tn) A.! idx in
            let tgt' = _deleteP key' tgt in
            if (tgt == tgt') then c else -- no change; short-circuit
            let bDel = isJust tgt && isNothing tgt' in
            let branch' = trie_branch tn A.// [(idx, tgt')] in
            let tn' = tn { trie_branch = branch' } in
            collapseIf vc bDel tn'

-- | Insert a single key,value pair into the trie, replacing any 
-- existing value at that location.
insert :: (VCacheable a) => ByteString -> a -> Trie a -> Trie a
insert k a = adjust (const (Just a)) k
{-# INLINE insert #-}

-- | Insert a list of (key,value) pairs into the trie. At the moment
-- this is just a linear insert, but it may later be replaced by an
-- efficient batch-insert model. If a key appears more than once in
-- this list, the last entry will win.
insertList :: (VCacheable a) => [(ByteString, a)] -> Trie a -> Trie a
insertList = flip $ L.foldl' ins where
    ins t (k,v) = insert k v t
{-# INLINE insertList #-}

-- | Remove a single key from the trie.
delete :: (VCacheable a) => ByteString -> Trie a -> Trie a
delete k = adjust (const Nothing) k
{-# INLINE delete #-}

-- | Remove a collection of keys from the trie. At the moment this is
-- just a sequential deletion, but it may later be replaced by a more
-- efficient batch-deletion model.
deleteList :: (VCacheable a) => [ByteString] -> Trie a -> Trie a 
deleteList = flip (L.foldl' (flip delete))
{-# INLINE deleteList #-}

-- | Update an element in the Trie with a given function. 
-- Capable of inserts, modifies, and deletes.
adjust :: (VCacheable a) => (Maybe a -> Maybe a) -> ByteString -> Trie a -> Trie a
adjust fn k t = runStrict $ adjustM fn' k t where
    fn' a = return (fn a)
{-# INLINE adjust #-}

-- | Adjust using an arbitrary action.
adjustM :: (VCacheable a, Monad m) => (Maybe a -> m (Maybe a)) -> ByteString -> Trie a -> m (Trie a)
adjustM fn k0 tr = adjustRoot where
    vc = trie_space tr
    adjustRoot = 
        wc k0 (trie_root tr) >>= \ c' ->
        return (Trie c' vc)
    wc key Nothing = fn Nothing >>= \ r -> case r of
        Nothing -> return Nothing
        Just a -> 
            let tn' = singletonNode key a in
            let c' = Just $! vref vc tn' in
            return $! c'
    wc key c@(Just pChild) =
        let tn = deref' pChild in
        let pre = trie_prefix tn in
        let s = sharedPrefixLen key pre in
        let p = B.length pre in
        let k = B.length key in
        if (s < p) -- need to split existing prefix in trie
           then fn Nothing >>= \ r -> 
                if isNothing r then return c else -- no change
                let ixP = B.index pre s in
                let tnP = tn { trie_prefix = B.drop (s+1) pre } in 
                let cP  = Just $! vref vc tnP in
                if (s == k)
                   then -- split prefix inline
                        let br' = mkChildren [(ixP, cP)] in
                        let tn' = Node br' key r in
                        let c' = Just $! vref vc tn' in
                        return $! c'
                   else -- branch middle of prefix
                        let ixK = B.index key s in
                        assert ((s < k) && (ixK /= ixP)) $
                        let brK = mkChildren [] in
                        let tnK = Node brK (B.drop (s+1) key) r in
                        let cK = Just $! vref vc tnK in
                        let br' = mkChildren [(ixK,cK), (ixP, cP)] in
                        let tn' = Node br' (B.take s pre) Nothing in
                        let c' = Just $! vref vc tn' in
                        return $! c'
           else if (s < k)
                   then -- adjust recursively
                        let key' = B.drop (s+1) key in
                        let ixK  = B.index key s in
                        let cK = (trie_branch tn) A.! ixK in
                        wc key' cK >>= \ cK' ->
                        if (cK == cK') then return c else -- no change!
                        let bDel = isJust cK && isNothing cK' in
                        let br' = trie_branch tn A.// [(ixK, cK')] in
                        let tn' = tn { trie_branch = br' } in
                        return $! collapseIf vc bDel tn'
                   else assert (s == k) $ -- adjust this node
                        let v = trie_accept tn in
                        fn v >>= \ v' ->
                        let bDel = isJust v && isNothing v' in
                        let tn' = tn { trie_accept = v' } in
                        return $! collapseIf vc bDel tn'

-- utility, tryCollapse only if some deletion condition is true
collapseIf :: (VCacheable a) => VSpace -> Bool -> Node a -> Child a
collapseIf vc bDel tn =
    if not bDel then Just $! vref vc tn else
    case tryCollapse tn of
        Nothing -> Nothing
        Just tn' -> Just $! vref vc tn'

-- tryCollapse will reconstruct a node after a deletion to preserve
-- invariant structure (i.e. that every node accepts or branches).
tryCollapse :: Node a -> Maybe (Node a)
tryCollapse tn =
    if isJust (trie_accept tn) then Just tn else -- node accepts
    let lChildren = L.filter (isJust . snd) (A.assocs (trie_branch tn)) in
    case lChildren of
        [] -> Nothing -- full collapse 
        [(ix, Just c)] -> -- need to collapse nodes linearly
            let tnC = deref' c in -- note: assuming tnC is valid
            let key' = toKey $ BB.byteString (trie_prefix tn)
                            <> BB.word8 ix
                            <> BB.byteString (trie_prefix tnC)
            in
            let tn' = tnC { trie_prefix = key' } in
            Just tn'
        _ -> Just tn -- node branches


-- TODO:
--
-- efficient bulk insert and deletion
-- efficient structural diff of tries


-- | Validate the invariant structure of the Trie. 
-- Every node must branch or contain a value.
validate :: Trie a -> Bool
validate = maybe True validRef . trie_root where
    validRef = validNode . deref'
    validNode tn = 
        let lChildren = catMaybes (A.elems (trie_branch tn)) in
        let bBranch = case lChildren of { (_:_:_) -> True; _ -> False } in
        let bAccept = isJust (trie_accept tn) in
        let bNodeValid = bAccept || bBranch in
        bNodeValid && L.all validRef lChildren

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


map :: (VCacheable b) => (a -> b) -> Trie a -> Trie b
map = mapWithKey . skip1st
{-# INLINE map #-}

mapM :: (VCacheable b, Monad m) => (a -> m b) -> Trie a -> m (Trie b)
mapM = mapWithKeyM . skip1st
{-# INLINE mapM #-}

mapWithKey :: (VCacheable b) => (ByteString -> a -> b) -> Trie a -> Trie b
mapWithKey fn = runStrict . mapWithKeyM fn' where
    fn' a b = pure (fn a b)
{-# INLINE mapWithKey #-}

mapWithKeyM :: (Monad m, VCacheable b) => (ByteString -> a -> m b) -> Trie a -> m (Trie b)
mapWithKeyM ff = wr where
    wr (Trie c vc) = 
        wc mempty c >>= \ c' ->
        return (Trie c' vc)
    wc _ Nothing = return Nothing
    wc p (Just c) =
        let tn = deref' c in
        let p' = nodePrefix p tn in -- extended prefix
        mbrun (ff (toKey p')) (trie_accept tn) >>= \ accept' ->
        let lcs = A.assocs (trie_branch tn) in
        M.mapM (wlc p') lcs >>= \ lcs' ->
        let branch' = A.array (minBound, maxBound) lcs' in
        let tn' = Node branch' (trie_prefix tn) accept' in
        let c' = vref' (vref_space c) tn' in
        return $! (Just $! c')
    wlc p (ix, child) =
        let p' = p <> BB.word8 ix in
        wc p' child >>= \ child' ->
        return (ix, child')

mbrun :: (Monad m) => (a -> m b) -> Maybe a -> m (Maybe b)
mbrun _ Nothing = return Nothing
mbrun action (Just a) = M.liftM Just (action a)

toKey :: BB.Builder -> ByteString
toKey = LB.toStrict . BB.toLazyByteString      

nodePrefix ::  BB.Builder -> Node a -> BB.Builder
nodePrefix k tn = 
    let p = trie_prefix tn in
    if B.null p then k else 
    k <> BB.byteString p 

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


