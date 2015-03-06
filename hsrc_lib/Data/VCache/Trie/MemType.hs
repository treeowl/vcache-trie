
-- The type structure for Mem trie should directly reflect the 
-- VCache layer trie (for easy merge, etc.).
module Data.VCache.Trie.MemType
    ( Trie(..)
    , Node(..)
    , Children
    , Child
    ) where

import Data.Word
import Data.ByteString (ByteString)
import qualified Data.Array.IArray as A

data Node a = Node
    { trie_branch :: {-# UNPACK #-} !(Children a)
    , trie_prefix :: {-# UNPACK #-} !ByteString 
    , trie_accept :: !(Maybe a)
    }
type Children a = A.Array Word8 (Child a)
type Child a = Maybe (Node a)
newtype Trie a = Trie { trie_root :: Child a }
