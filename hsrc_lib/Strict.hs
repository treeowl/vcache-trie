
-- | a monad that strictly evaluates intermediate return values.
module Strict (Strict(..)) where

import Control.Applicative
-- import Control.Monad

newtype Strict a = Strict { runStrict :: a }
instance Functor Strict where 
    fmap f (Strict a) = Strict (f a)
instance Applicative Strict where 
    pure = Strict
    (<*>) (Strict f) (Strict a) = Strict (f a)
instance Monad Strict where
    return = Strict
    (>>=) (Strict a) fn = fn a
