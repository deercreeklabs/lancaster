* [About](#about)
* [Installation](#installation)
* [Getting Started](#getting-started)
* [API Documentation](#api-documentation)
* [License](#license)

# About
Lancaster is an [Apache Avro](http://avro.apache.org/docs/current/)
library for Clojure and ClojureScript. It aims to be fully compliant
with the [Avro Specification](http://avro.apache.org/docs/current/spec.html).
It is assumed that the reader of this documentation is familiar with
Avro and Avro terminology. If this is your first exposure to Avro,
please read the [Avro Overview](http://avro.apache.org/docs/current/index.html)
and the  [Avro Specification](http://avro.apache.org/docs/current/spec.html)
before proceeding.

Lancaster provides functions for:
* Schema creation and manipulation
* Serialization to a byte array
* Deserialization from a byte array, including
[schema resolution](http://avro.apache.org/docs/current/spec.html#Schema+Resolution)
* Conversion from Lancaster schemas to
[Plumatic schemas](https://github.com/plumatic/schema) (spec support is
planned).

Lancaster does not support:
* Avro RPC and Protocols (RPC and messaging functionality is available
in [Capsule](https://github.com/deercreeklabs/capsule))
* Avro container files (may be supported in the future).

## Performance
Lancaster aims to be fast. Microbenchmarks show that it is generally faster
than JSON encoding while producing output that is much more compact.
The output of an unscientific run of the microbenchmark in
`deercreeklabs.perf-test` is pasted in below. Your mileage may vary.

Clojure | Value
-----------------------------------------
Avro encode ops per sec |          228833
Avro decode ops per sec |          380228
JSON encode ops per sec |          160000
JSON decode ops per sec |          234742
Deflated JSON encode ops per sec | 30120
Avro encoded size |                14
JSON encoded size |                142
Deflated JSON encoded size |       105

ClojureScript on Node.js |
-----------------------------------------
gAvro encode ops per sec |          35211
Avro decode ops per sec |          62112
JSON encode ops per sec |          36765
JSON decode ops per sec |          11211
Deflated JSON encode ops per sec | 2890
Avro encoded size |                14
JSON encoded size |                162
Deflated JSON encoded size |       109

## Project Name
The [Avro Lancaster](https://en.wikipedia.org/wiki/Avro_Lancaster) was an
airplane manufactured by the
[Avro Corporation](https://en.wikipedia.org/wiki/Avro).

# Installation
Using Leiningen / Clojars:

[![Clojars Project](http://clojars.org/deercreeklabs/lancaster/latest-version.svg)](http://clojars.org/deercreeklabs/lancaster)

# Getting Started

# API Documentation
All public functions are in the `deercreeklabs.lancaster` namespace. All
other namespaces should be considered private implementation details that
may change.

## Creating and Manipulating Schema objects
Schema objects are required for Lancaster serialization and deserialization.
They can be created in two ways:
1. From an existing Avro schema in JSON format. To do this, use the
[json->schema](#json-schema) function. This is best if you are working
with externally defined schemas from another system or language.
2. By using Lancaster schema functions. If you want to define Avro schemas
using Clojure/ClojureScript, Lancaster lets you concisely create and combine
schemas in arbitrarily complex ways, as explained below.

## Predefined Primitive Schemas
Lancaster provides predefined schema objects for all the
[Avro primitives](http://avro.apache.org/docs/current/spec.html#schema_primitive).
The following vars are defined in the `deercreeklabs.lancaster` namespace:
* `null-schema`
* `boolean-schema`
* `int-schema`
* `long-schema`
* `float-schema`
* `double-schema`
* `bytes-schema`
* `string-schema`

These schemas can be used directly or combined into complex schemas.

## Creating Complex Schemas
Lancaster provides the following functions and macros to create
[complex Avro schemas](http://avro.apache.org/docs/current/spec.html#schema_complex):
* [record-schema](#record-schema)
* [enum-schema](#enum-schema)
* [fixed-schema](#fixed-schema)
* [array-schema](#array-schema)
* [map-schema](#map-schema)
* [union-schema](#union-schema)

-------------------------------------------------------------------------------
### record-schema
```clojure
(record-schema name-kw fields)
```
Creates a Lancaster schema object representing an Avro
[```record```](http://avro.apache.org/docs/current/spec.html#schema_record),
with the given name keyword and field definitions. For a more
concise way to declare a record schema, see
[def-record-schema](#def-record-schema).

#### Parameters:
* `name-kw`: A keyword naming this ```record```. May or may not be
             namespaced. The name-kw must start with a letter and subsequently
             only contain letters, numbers, or hyphens.
* `fields`: A sequence of field definitions. Field definitions are sequences
            of the form ```[field-name-kw field-schema default-value]```.
    * `field-name-kw`: A keyword naming this field.
    * `field-schema`: A Lancaster schema object representing the field's schema.
    * `default-value`: Optional. The default data value for this field.

#### Return Value:
The new Lancaster record schema.

#### Example
```clojure
(def person-schema
  (l/record-schema :person
                   [[:name l/string-schema "no name"]
                    [:age l/int-schema]]))
```

-------------------------------------------------------------------------------
### enum-schema
```clojure
(enum-schema name-kw symbols)
```
Creates a Lancaster schema object representing an Avro
[```enum```](http://avro.apache.org/docs/current/spec.html#Enums),
with the given name and symbols. For a more
concise way to declare an enum schema, see
[def-enum-schema](#def-enum-schema).

#### Parameters:
* `name-kw`: A keyword naming this ```enum```. May or may not be
             namespaced. The name-kw must start with a letter and subsequently
             only contain letters, numbers, or hyphens.
* `symbols`: A sequence of keywords, representing the symbols in
             the enum

#### Return Value:
The new Lancaster enum schema.

#### Example
```clojure
(def suite-schema
  (l/enum-schema :suite [:clubs :diamonds :hearts :spades]))
```

-------------------------------------------------------------------------------
### fixed-schema
```clojure
(fixed-schema name-kw size)
```
Creates a Lancaster schema object representing an Avro
 [```fixed```](http://avro.apache.org/docs/current/spec.html#Fixed),
   with the given name and size. For a more
   concise way to declare a fixed schema, see [[def-fixed-schema]].

#### Parameters:
* `name-kw`: A keyword naming this ```fixed```. May or may not be
             namespaced. The name-kw must start with a letter and subsequently
             only contain letters, numbers, or hyphens.
* `size`: An integer representing the size of this fixed in bytes.

#### Return Value:
The new Lancaster fixed schema.

#### Example
```clojure
(def md5-schema
  (l/fixed-schema :md5 16))
```

-------------------------------------------------------------------------------
### array-schema
```clojure
(array-schema items-schema)
```
Creates a Lancaster schema object representing an Avro
[```array```](http://avro.apache.org/docs/current/spec.html#Arrays)
with the given items schema.

#### Parameters:
* `items-schema`: A Lancaster schema object describing the items in the array.

#### Return Value:
The new Lancaster array schema.

#### Example
```clojure
(def numbers-schema (l/array-schema l/int-schema))
```

-------------------------------------------------------------------------------
### map-schema
```clojure
(map-schema values-schema)
```
Creates a Lancaster schema object representing an Avro
[```map```](http://avro.apache.org/docs/current/spec.html#Maps)
with the given values schema.

#### Parameters:
* `values-schema`: A Lancaster schema object describing the values in the map.
Map keys are always strings.

#### Return Value:
The new Lancaster map schema.

#### Examples
```clojure
(def name->age-schema (l/map-schema l/int-schema))
```

-------------------------------------------------------------------------------
### union-schema
```clojure
(union-schema member-schemas)
```
Creates a Lancaster schema object representing an Avro
[```union```](http://avro.apache.org/docs/current/spec.html#Unions)
with the given member schemas.

#### Parameters:
* `members-schemas`: A sequence of Lancaster schema objects that are the
members of the union.

#### Return Value:
The new Lancaster union schema.

#### Examples
```clojure
(def maybe-name-schema
  (l/union-schema [l/null-schema l/string-schema]))
```

-------------------------------------------------------------------------------
### merge-record-schemas
```clojure
(merge-record-schemas name-kw schemas)
```
Creates a Lancaster record schema which contains all the fields
of all record schemas passed in.

#### Parameters:
* `name-kw`: A keyword naming the new combined record schema. May or may not be
             namespaced. The name-kw must start with a letter and subsequently
             only contain letters, numbers, or hyphens.
* `schemas`: A sequence of Lancaster schema record objects to be merged.

#### Return Value:
The new Lancaster record schema.

#### Example
```clojure
(l/def-record-schema person-schema
  [:name l/string-schema]
  [:age l/int-schema])

(l/def-record-schema location-schema
  [:latitude l/double-schema]
  [:longitude l/double-schema])

(def person-w-lat-long-schema
  (l/merge-record-schemas [person-schema location-schema]))
```

-------------------------------------------------------------------------------
### maybe
```clojure
(maybe schema)
```
Creates a Lancaster union schema whose members are l/null-schema
and the given schema. Makes a schema nillable.

#### Parameters:
* `schema`: The Lancaster schema to be made nillable.

#### Return Value:
The new Lancaster union schema.

#### Example
```clojure
(def int-or-nil-schema (l/maybe l/int-schema))
```

-------------------------------------------------------------------------------
### serialize
```clojure
(serialize writer-schema data)
```
Serializes data to a byte array, using the given Lancaster schema.

#### Parameters:
* `writer-schema`: The Lancaster schema that describes the data to be written.
* `data`: The data to be written.

#### Return Value:
A byte array containing the Avro-encoded data.

#### Example
```clojure
(l/def-record-schema person-schema
  [:name l/string-schema]
  [:age l/int-schema])

(def encoded (l/serialize person-schema {:name "Arnold"
                                         :age 22}))
```

### deserialize
```clojure
(deserialize reader-schema writer-schema ba)
```
Deserializes Avro-encoded data from a byte array, using the given reader and
writer schemas. The writer schema must be resolvable to the reader schema. See
[Avro Schema Resolution](http://avro.apache.org/docs/current/spec.html#Schema+Resolution).
If the reader schema contains
record fields that are not in the writer's schema, the fields' default values
will be used. If no default was explicitly specified in the schema, Lancaster
uses the following default values, depending on the field type:
* `null`: `nil`
* `boolean`: `false`
* `int`: `-1`
* `long`: `-1`
* `float`: `-1.0`
* `double`: `-1.0`
* `string`: `""`
* `enum`: first symbol in the schema's symbols list
* `array`: `[]`
* `map`: `{}`
* `flex-map`: `{}`

#### Parameters:
* `reader-schema`: The reader's Lancaster schema for the data
* `writer-schema`: The writer's Lancaster schema for the data
* `ba`: A byte array containing the encoded data

#### Return Value
The deserialized data.

#### Example
```clojure
(def person-schema
  (l/record-schema :person
                   [[:name l/string-schema "no name"]
                    [:age l/int-schema]]))

(def person-w-nick-schema
  (l/record-schema :person
                   [[:name l/string-schema "no name"]
                    [:age l/int-schema]
                    [:nickname l/string-schema "no nick"]
                    [:favorite-number l/int-schema]]))

(def encoded (l/serialize person-schema {:name "Alice"
                                         :age 20}))

(l/deserialize person-w-nick-schema person-schema encoded)
;; {:name "Alice", :age 20, :nickname "no nick", :favorite-number -1}
```

-------------------------------------------------------------------------------
### deserialize-same
```clojure
(deserialize-same schema ba)
```
Deserializes Avro-encoded data from a byte array, using the given schema
as both the reader and writer schema.

**Note that this is not recommended**, since the original writer's schema
should always be used to deserialize. The writer's schema
(in [Parsing Canonical Form](http://avro.apache.org/docs/current/spec.html#Parsing+Canonical+Form+for+Schemas))
should always be stored or transmitted with encoded data.

See also [deserialize](#deserialize).

#### Parameters:
* `schema`: The reader's and writer's Lancaster schema for the data
* `ba`: A byte array containing the encoded data

#### Return Value
The deserialized data.

#### Example
```clojure
(l/def-record-schema dog-schema
  [:name l/string-schema]
  [:owner l/string-schema])

(def encoded (l/serialize dog-schema {:name "Fido"
                                      :owner "Roger"}))

(l/deserialize-same dog-schema encoded)
;; {:name "Fido :owner "Roger"}
```

-------------------------------------------------------------------------------
### json->schema
```clojure
(json->schema json)
```
Creates a Lancaster schema object from an Avro schema in JSON format.

#### Parameters:
* `json`: A JSON string representing the Avro schema. The JSON string
must comply with the
[Avro Specification](http://avro.apache.org/docs/current/spec.html).

#### Return Value:
The new Lancaster schema.

#### Example
```clojure
(def person-schema
        (l/json->schema
         (str "{\"name\":\"Person\",\"type\":\"record\",\"fields\":"
              "[{\"name\":\"name\",\"type\":\"string\",\"default\":\"no name\"},"
              "{\"name\":\"age\",\"type\":\"int\",\"default\":-1}]}")))
```

-------------------------------------------------------------------------------
### wrap
```clojure
(wrap data-schema data)
```
Wraps the given data for use in an ambigous union. See
[Special Notes About Unions](#special-notes-about-unions) for more information.

#### Parameters:
* `data-schema`: The Lancaster schema of the data to be wrapped
* `data`: The data to be wrapped.

#### Return Value
The wrapped data.

#### Example
```clojure
(l/def-record-schema person-schema
  [:name l/string-schema "No name"]
  [:age l/int-schema 0])

(l/def-record-schema dog-schema
  [:name l/string-schema]
  [:owner l/string-schema])

(def person-or-dog-schema
  (l/union-schema [person-schema dog-schema]))

(def fido {:name "Fido" :owner "Roger"})

;; Serializing without wrapping fails because the union is ambiguous:
(l/serialize person-or-dog-schema fido)
;; ExceptionInfo Union requires wrapping, but data is not wrapped.

;; Wrapping the data before serialization tells the union which type
;; to use when serializing.
(def wrapped-fido (l/wrap dog-schema fido))

;; This works now
(l/serialize person-or-dog-schema wrapped-fido)
;; #object["[B" 0x2cc2072e "[B@2cc2072e"]
```

# License

Copyright (c) 2017-2019 Deer Creek Labs, LLC
*Apache Avro, Avro, Apache, and the Avro and Apache logos are trademarks of The Apache Software Foundation.*

Distributed under the Apache Software License, Version 2.0
http://www.apache.org/licenses/LICENSE-2.0.txt
