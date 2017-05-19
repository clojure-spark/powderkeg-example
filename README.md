# powderkeg-example

A Clojure library designed to ... well, that part is up to you.

## Usage

```clojure
user=> (require '[net.cgrand.xforms :as x])
nil
user=> (keg/connect! "local[2]")

user=> (into [] ; no collect, plain Clojure
  #_=>   (keg/rdd ["This is a firest line"  ; here we provide data from a clojure collection.
  #_=>             "Testing spark"
  #_=>             "and powderkeg"
  #_=>             "Happy hacking!"]
  #_=>    (filter #(.contains % "spark"))))

["Testing spark"]

user=> (into [] (keg/rdd (range 10)))
[0 1 2 3 4 5 6 7 8 9]
```
## License

Copyright © 2017 FIXME

Distributed under the Eclipse Public License either version 1.0 or (at
your option) any later version.
