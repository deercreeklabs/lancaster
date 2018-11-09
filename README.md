* [About](#about)
* [Installation](#installation)
* [Getting Started](#getting-started)
* [API Documentation](#api-documentation)
* [License](#license)

# About
Lancaster is an [Apache Avro](http://avro.apache.org/docs/current/)
library for Clojure and Clojurescript. It aims to be fully compliant
with the [Avro Specification](http://avro.apache.org/docs/current/spec.html).
It is assumed that the reader of this documentation is familiar with
Avro and Avro terminology. If this is your first exposure to Avro,
please read the [Avro Overview](http://avro.apache.org/docs/current/index.html)
and the  [Avro Specification](http://avro.apache.org/docs/current/spec.html)
before proceeding.

Lancaster provides functions for:
* Schema creation and manipulation
* Serialization to a byte array
* Deserialization from a byte array, including schema evolution
* Conversion from Lancaster schemas to Plumatic schemas (spec support is
planned).

Lancaster does not support:
* Avro RPC & Protocols (though similar / better functionality is available
in [Capsule](https://github.com/deercreeklabs/capsule))
* Avro container files (may be supported in the future).

## Performance
Lancaster aims to be fast. Microbenchmarks show that it is generally faster
than JSON encoding while producing much more compact output. See the
`deercreeklabs.perf-test` namespace for benchmarks.

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
1. From an existing Avro schema in JSON format

To do this, use the [json->schema](#json-schema) function. This is best if
you are working with externally defined schemas from another system
or language.
2. By using Lancaster schema functions.

If you want to define Avro schemas using Clojure(script), Lancaster lets you
concisely create and combine schemas in arbitrarily complex ways.

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
Lancaster provides functions and macros to create
[complex Avro schemas](http://avro.apache.org/docs/current/spec.html#schema_complex):
* [record-schema](#record-schema)
* [enum-schema](#enum-schema)

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
                   [[:name l/string-schema \"no name\"]
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

## Serializaion and Deserialization

# License

Copyright (c) 2017-2019 Deer Creek Labs, LLC
*Apache Avro, Avro, Apache, and the Avro and Apache logos are trademarks of The Apache Software Foundation.*

Distributed under the Apache Software License, Version 2.0
http://www.apache.org/licenses/LICENSE-2.0.txt
