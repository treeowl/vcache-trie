{-# LANGUAGE DeriveDataTypeable, BangPatterns #-}

-- | The underlying structure for a Trie. 
--
-- This is an internal module, but is exposed for now because I'm not
-- sure how to best generically target features such as structural 
-- diffs. And we might eventually benefit from zippers, etc. After 
-- experimenting and learning, I ask you to push the best generic 
-- code into the vcache-trie package (i.e. send a pull request).
module Data.VCache.Trie.Type
    ( Trie(..)
    , Node(..)
    , Children
    , Child
    , unsafeTrieAddr
    -- , ZPath, ZSeek, ZFind(..), keyPath

    -- utility
    , sharedPrefixLen
    ) where

import Control.Applicative
import Data.Word
import qualified Data.Array.IArray as A
import Data.ByteString (ByteString)
import qualified Data.ByteString.Internal as BSI
import Data.Typeable
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
-- Children are serialized as a simple [(Word8, VRef Node)] list.
-- The high branching factor favors sparse, flat tries, which is
-- useful because VCache taxes deep lookups more than looking up
-- larger values. 
--


-- | A node should either accept a value or branch into at least two
-- children. 
data Node a = Node
    { trie_branch :: {-# UNPACK #-} !(Children a) -- arity 256; one byte from prefix
    , trie_prefix :: {-# UNPACK #-} !ByteString   -- compact extended prefix
    , trie_accept :: !(Maybe a)                   -- value associated with prefix
    } deriving (Eq, Typeable)
-- Invariant for nodes: either we accept or we have at least two children

type Children a = A.Array Word8 (Child a)
type Child a = Maybe (VRef (Node a))


-- | A trie data structure with bytestring keys, above VCache.
-- 
-- A trie supports keys of arbitrary size, though very large keys may
-- cause performance degradation. Values are directly serialized into
-- nodes, so very large values should use indirection.
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
    put (Trie r vc) = put r >> put vc
instance Show (Trie a) where
    showsPrec _ t = showString "Trie#" . shows (unsafeTrieAddr t)

-- | Obtain unique address for Trie value. As with VRef addresses, this
-- should be stable while the Trie is reachable, but may change if the
-- value is GC'd and later reconstructed at a new address. Exposed for
-- memoization and similar purposes. 
unsafeTrieAddr :: Trie a -> Word64
unsafeTrieAddr = maybe 0 unsafeVRefAddr . trie_root
{-# INLINE unsafeTrieAddr #-}

mkChildren :: [(Word8, VRef (Node a))] -> Children a
mkChildren = A.accumArray ins Nothing (minBound, maxBound) where
    ins _ c = Just c

getChildren :: (VCacheable a) => VGet (Children a)
getChildren = mkChildren <$> get -- get a list of pairs

listChildren :: Children a -> [(Word8, VRef (Node a))]
listChildren = mapMaybe toChild . A.assocs where
    toChild (_, Nothing) = Nothing
    toChild (ix, Just c) = Just (ix, c)

putChildren :: (VCacheable a) => Children a -> VPut ()
putChildren = put . listChildren

{-

-- | ZPath a corresponds to a deep search to find a key.
-- It returns a zipper-like data structure.
type ZPath a = (ZSeek a, ZFind a)
type ZSeek a = [(Word8, Node a)] -- a simple stack of nodes

data ZFind a 
    = ZTerm !ByteString -- ^ bytes not matched at end of trie
    | ZSplit !ByteString {-# UNPACK #-} !SharedPrefixLen !(Node a) -- ^ match between nodes
    | ZFind !(Node a) -- ^ exact match to existing node

type SharedPrefixLen = Int

-- | compute the path to a node as a data structure.
keyPath :: ByteString -> Trie a -> ZPath a
keyPath = kr where
    kr key = kc [] key . trie_root
    kc zs key Nothing = (zs, ZTerm key)
    kc zs key (Just c) =
        let tn = deref' c in
        let pre = trie_prefix tn in
        let s = sharedPrefixLen key pre in
        let p = B.length pre in
        let k = B.length key in
        if (s < p) then (zs, ZSplit key s tn) else
        assert (s == p) $
        if (s == k) then (zs, ZFind tn) else
        assert (s < k) $ 
        let key' = B.drop (s+1) key in
        let ixK = B.index key s in
        kc ((ixK,tn):zs) key' (trie_branch tn A.! ixK)

-}

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


