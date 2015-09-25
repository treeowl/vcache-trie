{-# LANGUAGE BangPatterns, PatternGuards, DeriveDataTypeable #-}
-- | A compact bytestring trie implemented above VCache.
module Data.VCache.Trie
    ( Trie
    , trie_space
    , empty, singleton
    , null, size
    , lookup, lookup'
    , prefixKeys
    , lookupPrefix, lookupPrefix'
    , lookupPrefixNode, lookupPrefixNode' 
    , deletePrefix
    , insert, delete, adjust
    , insertList, deleteList

    , toList, toListBy, elems, keys
    , foldr, foldr', foldrM, foldrWithKey, foldrWithKey', foldrWithKeyM
    , foldl, foldl', foldlM, foldlWithKey, foldlWithKey', foldlWithKeyM

    , map, mapM, mapWithKey, mapWithKeyM 

    , toListOnKey
    , diff, Diff(..)

    , validate
    , unsafeTrieAddr
    , DerefNode
    ) where

import Prelude hiding (null, lookup, foldr, foldl, map, mapM, elem)
import Control.Applicative hiding (empty)
import qualified Control.Monad as M
import Control.Exception (assert)
import Data.Word
import Data.ByteString (ByteString)
import qualified Data.ByteString as B
import qualified Data.Array.IArray as A
import qualified Data.List as L
import Data.Maybe
import Data.Monoid

import Strict
import Ident
import Data.VCache.Trie.Type

import Database.VCache

-- | function to dereference a Trie cache node.
-- This improves user control over caching on lookup.
type DerefNode a = VRef (Node a) -> Node a

-- default dereference for lookups
defaultDeref :: DerefNode a
defaultDeref = derefc CacheMode0

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

-- | Lookup an object by key with user-provided deref.
lookup' :: DerefNode a -> ByteString -> Trie a -> Maybe a
lookup' d k = _lookup d k . trie_root

-- | Lookup an object by key
lookup :: ByteString -> Trie a -> Maybe a
lookup = lookup' defaultDeref

_lookup :: DerefNode a -> ByteString -> Child a -> Maybe a
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
-- This operation may need to allocate in VCache, e.g. to delete
-- some fraction of the requested prefix. This isn't optimal for
-- performance.
lookupPrefix :: (VCacheable a) => ByteString -> Trie a -> Trie a
lookupPrefix = lookupPrefix' defaultDeref 

-- | lookup prefix with user-provided deref function.
lookupPrefix' :: (VCacheable a) => DerefNode a -> ByteString -> Trie a -> Trie a
lookupPrefix' d k tr = 
    let node = lookupPrefixNode' d k tr in
    let vc = trie_space tr in
    let child = vref vc <$> node in
    Trie child vc

-- | Obtain a trie node rooted at a given prefix, if any content
-- exists at this prefix. This operation allows some performance 
-- benefits compared to 'lookupPrefix' because it never allocates
-- at the VCache layer. 
lookupPrefixNode :: (VCacheable a) => ByteString -> Trie a -> Maybe (Node a)
lookupPrefixNode = lookupPrefixNode' defaultDeref

-- | lookup prefix node with user-provided deref function
lookupPrefixNode' :: (VCacheable a) => DerefNode a -> ByteString -> Trie a -> Maybe (Node a)
lookupPrefixNode' d k = _lookupP d k . trie_root

_lookupP :: (VCacheable a) => DerefNode a -> ByteString -> Child a -> Maybe (Node a)
_lookupP d key c | B.null key = d <$> c -- stop on exact node
_lookupP _ _ Nothing = Nothing -- 
_lookupP d key (Just c) = 
    let tn = d c in
    let pre = trie_prefix tn in
    let s = sharedPrefixLen key pre in
    let k = B.length key in
    let p = B.length pre in
    if (k <= p) -- test whether key should fit within prefix
       then if (s < k) then Nothing else
            assert (s == k) $
            let pre' = B.drop k pre in -- trim key from prefix
            let tn' = tn { trie_prefix = pre' } in
            Just $! tn' -- allocate new node
       else if (s < p) then Nothing else 
            assert (s == p) $
            let key' = B.drop (p+1) key in
            let c' = (trie_branch tn) A.! (B.index key p) in
            _lookupP d key' c' -- recursive lookup

-- | Delete all keys sharing a given prefix. 
deletePrefix :: (VCacheable a) => ByteString -> Trie a -> Trie a
deletePrefix = deletePrefix' deref'

-- | Delete keys using user-defined lookup.
deletePrefix' :: VCacheable a => DerefNode a -> ByteString -> Trie a -> Trie a
deletePrefix' d p (Trie c vc) = Trie (_deleteP d p c) vc

_deleteP :: (VCacheable a) => DerefNode a -> ByteString -> Child a -> Child a 
_deleteP _ _ Nothing = Nothing
_deleteP d key c@(Just pNode) = 
    let vc = vref_space pNode in
    let tn = d pNode in
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
            let tgt' = _deleteP d key' tgt in
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
            let key' = mconcat [trie_prefix tn, B.singleton ix, trie_prefix tnC] in
            let tn' = tnC { trie_prefix = key' } in
            Just tn'
        _ -> Just tn -- node branches


-- TODO:
--
-- efficient bulk insert and deletion
-- efficient structural diff of tries
-- ranged key or element searches


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
        let p' = p <> trie_prefix tn in
        wlc p' (trie_branch tn) maxBound b >>=
        maybe return (ff p') (trie_accept tn)
    wlc p a !k b = 
        let cc = if (minBound == k) then return else wlc p a (k-1) in
        let p' = p `B.snoc` k in -- branch prefix
        wc p' b (a A.! k) >>= cc
{-# NOINLINE foldrWithKeyM #-}

foldlWithKeyM :: (Monad m) => (b -> ByteString -> a -> m b) -> b -> Trie a -> m b
foldlWithKeyM ff = wr where
    wr b = wc mempty b . trie_root
    wc _ b Nothing = return b
    wc p b (Just c) =
        let tn = deref' c in
        let p' = p <> trie_prefix tn in -- extended prefix
        let cc = wlc p' (trie_branch tn) minBound in
        case trie_accept tn of
            Nothing -> cc b
            Just val -> ff b p' val >>= cc
    wlc p a !k b =
        let cc = if (maxBound == k) then return else wlc p a (k+1) in
        let p' = p `B.snoc` k in -- branch prefix
        wc p' b (a A.! k) >>= cc
{-# NOINLINE foldlWithKeyM #-}


-- | Quickly find keys to the left or right of a given key. If the given
-- key is matched, it appears at the head of the right list. The left list
-- is reverse-ordered, finding keys to the left of the requested key.
--
-- The intention here is to support efficient ranged searches or lookups.
-- The lists returned are computed lazily.
toListOnKey :: ByteString -> Trie a -> ([(ByteString, a)], [(ByteString, a)])
toListOnKey fullKey = nodeOnKey 0 . trie_root where
    nodeOnKey _ Nothing = ([],[])
    nodeOnKey nKeyBytes (Just c) = 
        let tn = deref' c in
        let key = B.drop nKeyBytes fullKey in
        let pre = trie_prefix tn in
        let s = sharedPrefixLen key pre in
        let pathToNode = B.take nKeyBytes fullKey in
        let nodeToLeftOfKey = (nodeL pathToNode tn, []) in
        let nodeToRightOfKey = ([], nodeR pathToNode tn) in
        -- if key is fully matched, then node is fully to right.
        if (s == B.length key) then nodeToRightOfKey else
        assert (s < B.length key) $
        let ck = B.index key s in
        -- if we've matched the whole prefix, but not the whole key,
        -- we can split the node.
        if (s == B.length pre) then nodeSplitOnKey (nKeyBytes + s) ck tn else
        assert (s < B.length pre) $
        -- otherwise, key is fully to left or right of current node.
        let cp = B.index pre s in
        if (cp > ck) then nodeToRightOfKey else 
        assert (cp < ck) $ nodeToLeftOfKey

    nodeElem p = maybe [] (return . (,) p) . trie_accept

    -- entire node is to left of key; fast path
    nodeL pathToNode tn =
        let p = pathToNode <> trie_prefix tn in
        let onC (i, c) = maybe [] (nodeL (p `B.snoc` i) . deref') c in
        let children = fmap onC $ A.assocs (trie_branch tn) in
        let elem = nodeElem p tn in
        mconcat $ L.reverse (elem:children) -- largest key at head

    -- entire node is to right of key; fast path
    nodeR pathToNode tn =
        let p = pathToNode <> trie_prefix tn in
        let onChild (i,c) = maybe [] (nodeR (p `B.snoc` i) . deref') c in
        let children = fmap onChild $ A.assocs (trie_branch tn) in
        let elem = nodeElem p tn in
        mconcat (elem:children) -- i.e. smallest key at head

    -- key splits node in twain, some to left and/or some to right
    nodeSplitOnKey nKeyBytes keyChar tn =
        let p = B.take nKeyBytes fullKey in
        let elem = nodeElem p tn in
        let onCL (i,c) = maybe [] (nodeL (p `B.snoc` i) . deref') c in
        let onCR (i,c) = maybe [] (nodeR (p `B.snoc` i) . deref') c in
        let cL = fmap onCL $ L.filter ((< keyChar) . fst) $ A.assocs (trie_branch tn) in
        let cR = fmap onCR $ L.filter ((> keyChar) . fst) $ A.assocs (trie_branch tn) in
        let eL = mconcat $ L.reverse (elem:cL) in -- element is leftmost from key
        let eR = mconcat cR in
        let (keL,keR) = nodeOnKey (nKeyBytes + 1) (trie_branch tn A.! keyChar) in
        (keL ++ eL, keR ++ eR)

-- | a simple difference data structure. We're either 
-- just in the left, just in the right, or have some
-- simple difference in both (e.g. based on Eq).
data Diff a = InL a | Diff a a | InR a
    deriving (Show, Eq)

-- thoughts: it might be worthwhile to have a variation
-- that preserves common subtrees and elements, i.e. such
-- that both trees can be reconstructed from the diff.

-- | Compute differences between two tries. The provided functions 
-- determine the difference type for values in just the left or right
-- or both. 
diff :: (Eq a) => Trie a -> Trie a -> [(ByteString, Diff a)]
diff = diffRoot where
    diffRoot a b = diffChild mempty (trie_root a) (trie_root b)

    diffChild _ Nothing Nothing = mempty
    diffChild k (Just a) Nothing = subtree k InL (deref' a)
    diffChild k Nothing (Just b) = subtree k InR (deref' b)
    diffChild k (Just a) (Just b) = 
        if (a == b) then mempty else 
        diffNode k (deref' a) (deref' b)

    diffNode k tnA tnB =
        let kA = trie_prefix tnA in
        let kB = trie_prefix tnB in
        let s = sharedPrefixLen kA kB in
        if (s == B.length kA) 
            then if (s == B.length kB) 
                then diffNodeEQ k tnA tnB
                else diffNodeAB k tnA tnB
            else if (s == B.length kB)
                then diffNodeBA k tnA tnB
                else let elemsA = subtree k InL tnA in
                     let elemsB = subtree k InR tnB in
                     let ca = B.index (trie_prefix tnA) s in
                     let cb = B.index (trie_prefix tnB) s in
                     -- nodes are for adjacent, non-overlapping subtrees
                     assert (ca /= cb) $
                     if (ca < cb) then elemsA <> elemsB
                                  else elemsB <> elemsA

    diffNodeAB k tnA tnB = -- A is strict prefix of B, higher in tree
        assert (trie_prefix tnA `isStrictPrefixOf` trie_prefix tnB) $
        let s = B.length (trie_prefix tnA) in
        let c = B.index (trie_prefix tnB) s in
        let (diffLeft, diffRight) = splitTree k InL c tnA in
        let diffMiddle = case trie_branch tnA A.! c of
                Nothing -> subtree k InR tnB
                Just childSplit ->  
                    let keySplit = k <> B.take (s + 1) (trie_prefix tnB) in
                    let tnB' = tnB { trie_prefix = B.drop (s + 1) (trie_prefix tnB) } in
                    diffNode keySplit (deref' childSplit) tnB'
        in
        mconcat [diffLeft, diffMiddle, diffRight]

    diffNodeBA k tnA tnB = -- B is strict prefix of A, higher in tree
        assert (trie_prefix tnB `isStrictPrefixOf` trie_prefix tnA) $
        let s = B.length (trie_prefix tnB) in
        let c = B.index (trie_prefix tnA) s in
        let (diffLeft, diffRight) = splitTree k InR c tnB in
        let diffMiddle = case trie_branch tnB A.! c of
                Nothing -> subtree k InL tnA
                Just childSplit ->
                    let keySplit = k <> B.take (s + 1) (trie_prefix tnA) in
                    let tnA' = tnA { trie_prefix = B.drop (s + 1) (trie_prefix tnA) } in
                    diffNode keySplit tnA' (deref' childSplit)
        in
        mconcat [diffLeft, diffMiddle, diffRight]

    diffNodeEQ k tnA tnB = -- nodes match on key including trie_prefix 
        assert (trie_prefix tnA == trie_prefix tnB) $
        let fullKey = k <> trie_prefix tnA in
        let diffK = case (trie_accept tnA, trie_accept tnB) of
                (Just va, Nothing) -> [(fullKey, InL va)]
                (Nothing, Just vb) -> [(fullKey, InR vb)]
                (Just va, Just vb) | (va /= vb) -> [(fullKey, Diff va vb)]
                _ -> [] -- no difference
        in
        let diffC (i, cA) = diffChild (fullKey `B.snoc` i) cA (trie_branch tnB A.! i) in
        let diffChildren = L.concatMap diffC (A.assocs (trie_branch tnA)) in
        diffK <> diffChildren

    -- common case where a whole subtree is different
    subtree k fn tn = -- list every element in trie, in left or right
        let fullKey = k <> trie_prefix tn in
        let lK = maybeToList $ fmap ((,) fullKey . fn) (trie_accept tn) in
        let onC (i,c) = maybe [] (subtree (fullKey `B.snoc` i) fn . deref') c in
        let lC = mconcat $ fmap onC $ A.assocs (trie_branch tn) in
        lK <> lC

    splitTree k fn c tn = -- split a subtree around a key
        let fullKey = k <> trie_prefix tn in
        let elemK = maybeToList $ fmap ((,) fullKey . fn) $ trie_accept tn in
        let onC i = maybe [] (subtree (fullKey `B.snoc` i) fn . deref') (trie_branch tn A.! i) in
        let rangeL = if (c == minBound) then [] else enumFromTo minBound (pred c) in
        let rangeR = if (c == maxBound) then [] else enumFromTo (succ c) maxBound in
        let leftElems = mconcat $ elemK : fmap onC rangeL in
        let rightElems = mconcat $ fmap onC rangeR in
        (leftElems, rightElems)
        
isStrictPrefixOf :: ByteString -> ByteString -> Bool
isStrictPrefixOf a b = (B.length a < B.length b) && (a `B.isPrefixOf` b)
       


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
        let p' = p <> trie_prefix tn in -- extended prefix
        mbrun (ff p') (trie_accept tn) >>= \ accept' ->
        let lcs = A.assocs (trie_branch tn) in
        M.mapM (wlc p') lcs >>= \ lcs' ->
        let branch' = A.array (minBound, maxBound) lcs' in
        let tn' = Node branch' (trie_prefix tn) accept' in
        let c' = vref' (vref_space c) tn' in
        return $! (Just $! c')
    wlc p (ix, child) =
        let p' = p `B.snoc` ix in
        wc p' child >>= \ child' ->
        return (ix, child')

mbrun :: (Monad m) => (a -> m b) -> Maybe a -> m (Maybe b)
mbrun _ Nothing = return Nothing
mbrun action (Just a) = M.liftM Just (action a)


