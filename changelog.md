
# Versions

## 0.1 
* initial release

## 0.1.1 
* flexibility: expose module VCache.Trie.Type

## 0.1.2 
* performance: lookup by prefix without allocation

## 0.2.0
* documentation: adding this changelog
* modified lookup' to receive user-defined 'deref' behavior
 * removes lookupc and modifies signature for lookup'
* lookup variants now use CacheMode0 by default