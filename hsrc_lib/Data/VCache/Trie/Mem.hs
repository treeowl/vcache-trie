
-- | To support efficient bulk operations (insertion, deletion, etc.)
-- it is useful to avoid unnecessary allocations at the VCache layer.
-- This is achievable by having easy integration with an in-memory
-- trie that has a structure corresponding to the VCache-layer Trie.
--
-- At the moment, this is just an idea for the future.
module Data.VCache.Trie.Mem
    ( Trie
    ) where

import Data.VCache.Trie.MemType



{-

maybe these should be in a `Data.VCache.Trie.Batch` module?

-- TODO: bulk inserts and conversions
toMemTrie :: VC.Trie a -> Mem.Trie a
toMemTrie = fmap (fromNode . deref') . trie_root where
    fromNode tn = 
        let lcs = A.assocs (trie_branch tn) in
        let lcs' = (`mapMaybe` lcs) $ \ (i,mbc) -> case mbc of
              Nothing -> Nothing
              Just c -> Just (i, fromNode (deref' c))
        in
        Mem.Node { Mem.trie_branch = lcs'
                 , Mem.trie_prefix = trie_prefix tn
                 , Mem.trie_accept = trie_accept tn
                 }

fromMemTrie :: Mem.Trie a -> VC.Trie a

-}

