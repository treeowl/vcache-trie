
module Ident (Ident(..)) where

import Control.Applicative
import Control.Monad

newtype Ident a = Ident { runIdent :: a }

instance Functor Ident where
    fmap fn m = Ident (fn (runIdent m))
instance Applicative Ident where
    pure = return
    (<*>) = ap
instance Monad Ident where
    return = Ident
    (>>=) m fn = fn (runIdent m)
