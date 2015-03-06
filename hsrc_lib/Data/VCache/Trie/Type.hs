{-# LANGUAGE DeriveDataTypeable #-}

module Data.VCache.Trie.Type
    ( Trie(..)
    , Node(..)
    , Children
    , Child
    , impossible
    ) where

import Control.Applicative
import Control.Monad
import Data.Word
import Data.Bits
import qualified Data.Array.IArray as A
import Data.ByteString (ByteString)
import Data.Typeable
import qualified Data.List as L
import Data.Maybe

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
instance Show (Trie a) where
    showsPrec _ = shows . trie_root



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



