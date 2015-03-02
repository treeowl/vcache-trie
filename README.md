
# Haskell VCache Trie

A Patricia trie implemented above [VCache](https://hackage.haskell.org/package/vcache). 

Suitable for database-as-a-value or modeling abstract virtual filesystems. 

Currently, this only supports bytestring keys, and follows the [bytestring-trie](http://hackage.haskell.org/package/bytestring-trie) package with regards to API. It may be necessary to later adapt the [list-tries](http://hackage.haskell.org/package/list-tries) model to support arbitrary keys... though, not for any projects I'm pursuing at the moment.
