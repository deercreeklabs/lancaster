* [Installation](#installation)
* [About](#about)
* [Getting Started](#getting-started)
* [API Documentation](#api-documentation)
* [License](#license)

# Installation
Using Leiningen / Clojars:

[![Clojars Project](http://clojars.org/deercreeklabs/lancaster/latest-version.svg)](http://clojars.org/deercreeklabs/lancaster)

# About
Lancaster is an [Apache Avro](http://avro.apache.org/docs/current/)
library for Clojure and ClojureScript and aims to be fully compliant
with the [Avro Specification](http://avro.apache.org/docs/current/spec.html).
It is assumed that the reader of this documentation is familiar with
Avro and Avro terminology. If this is your first exposure to Avro,
please read the [Avro Overview](http://avro.apache.org/docs/current/index.html)
and the [Avro Specification](http://avro.apache.org/docs/current/spec.html)
before proceeding.

Lancaster provides for:
* Easy schema creation
* Serialization of arbitrarily-complex data structures to a byte array
* Deserialization from a byte array, including
[schema resolution](http://avro.apache.org/docs/current/spec.html#Schema+Resolution)
* Conversion from Lancaster schemas to
[Plumatic schemas](https://github.com/plumatic/schema) (spec support is
planned).

Lancaster does not support:
* Avro protocols
* Avro container files (may be supported in the future).

## Performance
Lancaster aims to be fast. Microbenchmarks show that it is generally faster
than JSON serialization / deserialization while producing output
that is much more compact. The output of an unscientific run of
the microbenchmark in `deercreeklabs.perf-test` is pasted in below.
Your mileage may vary.

**Clojure 1.9 on JVM 1.8**

Parameter    | Lancaster |    JSON   | JSON+Deflate
:----------- | --------: |---------: | -----------:
Encode ops/s | `235,849` | `138,696` | `29,240`
Decode ops/s | `364,964` | `233,645` | `86,207`
Encoded size (bytes) | `14` | `142`  | `105`

**ClojureScript 1.10.339 on Node.js 8.10**

Parameter    | Lancaster |    JSON   | JSON+Deflate
:----------- | --------: |---------: | -----------:
Encode ops/s | `32,468`  | `30,769`  | `2,865`
Decode ops/s | `76,923`  | `10,428`  | `3,597`
Encoded size (bytes) | `14` | `162`  | `109`

## Project Name
The [Avro Lancaster](https://en.wikipedia.org/wiki/Avro_Lancaster) was an
airplane manufactured by [Avro Aircraft](https://en.wikipedia.org/wiki/Avro).


# Examples
Here's an introductory example of using Lancaster to define a schema,
serialize data, and then deserialize it again.

```clojure
(require '[deercreeklabs.lancaster :as l])

(l/def-record-schema person-schema
  [:name l/string-schema]
  [:age l/int-schema]
  [:dog-name (l/maybe l/string-schema)] ;; l/maybe makes field nillable
  [:favorite-numbers (l/array-schema l/int-schema)]) ;; array of ints

(def alice
  {:name "Alice"
   :age 40
   :favorite-numbers [12 89]})

(def encoded (l/serialize person-schema alice))

(l/deserialize person-schema person-schema encoded)
;; {:name "Alice" :age 40 :dog-name nil :favorite-numbers [12 89]}
```

# Creating Schema objects
Lancaster schema objects are required for serialization and deserialization.
This can be done in two ways:
1. Using an existing Avro schema in JSON format.
To do this, use the [json->schema](#json-schema) function. This is best if
you are working with externally defined schemas from another system or language.
2. Using Lancaster schema functions and/or macros.
This is best if you want to define Avro schemas using Clojure/ClojureScript.
Lancaster lets you concisely create and combine schemas in arbitrarily complex
ways, as explained below.

## Primitive Schemas
Lancaster provides predefined schema objects for all the
[Avro primitives](http://avro.apache.org/docs/current/spec.html#schema_primitive).
The following vars are defined in the `deercreeklabs.lancaster` namespace:
* `null-schema`: Represents an Avro `null`
* `boolean-schema`: Represents an Avro `boolean`
* `int-schema`: Represents an Avro `int`
* `long-schema`: Represents an Avro `long`
* `float-schema`: Represents an Avro `float`
* `double-schema`: Represents an Avro `double`
* `bytes-schema`: Represents an Avro `bytes`
* `string-schema`: Represents an Avro `string`

These schema objects can be used directly or combined into complex schemas.

## Complex Schemas
Most non-trivial Lancaster use cases will involve
[complex Avro schemas](http://avro.apache.org/docs/current/spec.html#schema_complex).
The easiest and most concise way to create complex
schemas is by using the [Schema Creation Macros](#schema-creation-macros).
For situations where macros do not work well,
the [Schema Creation Functions](#schema-creation-functions) are also
available.

### Schema Creation Macros
* [def-array-schema](#def-array-schema): Defines a var w/ an array schema.
* [def-enum-schema](#def-enum-schema): Defines a var w/ an enum schema.
* [def-fixed-schema](#def-fixed-schema): Defines a var w/ a fixed schema.
* [def-map-schema](#def-map-schema): Defines a var w/ a map schema. Keys must be strings.
* [def-flex-map-schema](#def-flex-map-schema): Defines a var w/ a flex-map schema. Flex map keys may be of any schema type.
* [def-record-schema](#def-record-schema): Defines a var w/ a record schema.
* [def-merged-record-schema](#def-merged-record-schema): Defines a var w/ a record schema which contains all the fields of all record schemas passed in.
* [def-union-schema](#def-union-schema): Defines a var w/ a union schema.
* [def-maybe-schema](#def-maybe-schema): Defines a var w/ a nillable schema.

### Schema Creation Functions
* [array-schema](#array-schema): Creates an array schema.
* [enum-schema](#enum-schema): Creates an enum schema.
* [fixed-schema](#fixed-schema): Creates a fixed schema.
* [map-schema](#map-schema): Creates a map schema. Keys must be strings.
* [flex-map-schema](#flex-map-schema): Creates a flex-map schema. Flex map keys may be of any schema type.
* [record-schema](#record-schema): Creates a record schema.
* [merged-record-schema](#merged-record-schema): Creates a record schema which contains all the fields of all record schemas passed in.
* [union-schema](#union-schema): Creates a union schema.
* [maybe](#schema): Creates a nillable schema.

## Operations on Schema Objects
All of these functions take a Lancaster schema object as the first argument
* [serialize](#serialize): Serializes data to a byte array.
* [deserialize](#deserialize): Deserializes data from a byte array, using separate reader and writer schemas. **This is the recommended deserialization function**.
* [deserialize-same](#deserialize-same): Deserializes data from a byte array, using the same reader and writer schema. **This is not recommended**, as it does not allow for [schema
resolution / evolution](http://avro.apache.org/docs/current/spec.html#Schema+Resolution).
* [wrap](#wrap): Wraps data for use in an ambiguous union. See [Notes About Union Data Types](#notes-about-union-data-types) below.
* [edn](#edn): Returns the EDN representation of the schema.
* [json](#json): Returns the JSON representation of the schema.
* [pcf](#pcf): Returns a JSON string containing the
[Parsing Canonical Form](http://avro.apache.org/docs/current/spec.html#Parsing+Canonical+Form+for+Schemas) of the schema.
* [fingerprint64](#fingerprint64): Returns the 64-bit [Rabin fingerprint](http://en.wikipedia.org/wiki/Rabin_fingerprint) of the [Parsing Canonical Form](http://avro.apache.org/docs/current/spec.html#Parsing+Canonical+Form+for+Schemas) of the schema.
* [schema?](#schema): Is the argument a Lancaster schema?
* [plumatic-schema](#plumatic-schema): Returns a [Plumatic schema](https://github.com/plumatic/schema) for the schema.
* [default-data](#default-data): Returns default data that conforms to the schema.

# Data types

**Serialization**

When serializing data, Lancaster accepts the following Clojure(Script)
types for the given Avro type:

Avro Type | Acceptable Clojure / ClojureScript Types
--------- | -------------------------
`null` | `nil`
`boolean` | `boolean`
`int` | `int`, `java.lang.Integer`, `long (if in integer range)`, `java.lang.Long (if in integer range)`, `js/Number (if in integer range)`
`long` | `long`, `java.lang.Long`
`float` | `float`, `java.lang.Float`, `double (if in float range)`, `java.lang.Double (if in float range)`, `js/Number (if in float range)`
`double` | `double`, `java.lang.Double`, `js/Number`
`bytes` | `byte-array`, `java.lang.String`, `js/Int8Array`, `js/String`
`string` | `byte-array`, `java.lang.String`, `js/Int8Array`, `js/String`
`fixed` | `byte-array`, `js/Int8Array`. Byte array length must equal the size declared in the creation of the Lancaster `fixed` schema.
`enum` | `keyword`
`array` | Any data that passes `(sequential? data)`
`map` | Any data that passes `(map? data)`, if all keys are strings. Clojure(Script) records *DO NOT* qualify, since their keys are keywords.
`record` | Any data that passes `(map? data)`, if all keys are Clojure(Script) keywords. Clojure(Script) records *DO* qualify, since their keys are keywords.
`union` | Any data that matches one of the member schemas declared in the creation of the Lancaster `union` schema. Note that some unions require wrapping, as explained in [Notes About Union Data Types](#notes-about-union-data-types)

**Deserialization**

When deserializing data, Lancaster returns the following Clojure or ClojureScript
types for the given Avro type:

Avro Type | Clojure Type | ClojureScript Type
--------- | ------------ | ------------------
`null` | `nil` | `nil`
`boolean` | `boolean` | `boolean`
`int` | `java.lang.Integer` | `js/Number`
`long` | `java.lang.Long` | `js/Number`
`float` | `java.lang.Float` | `js/Number`
`double` | `java.lang.Double` | `js/Number`
`bytes` | `byte-array` | `js/Int8Array`
`string` | `java.lang.String` | `js/String`
`fixed` | `byte-array` | `js/Int8Array`
`enum` | `keyword` | `keyword`
`array` | `vector` | `vector`
`map` | `hash-map` | `hash-map`
`record` | `hash-map` | `hash-map`
`union` | Data that matches one of the member schemas declared in the creation of the Lancaster `union` schema. If the union schema requires wrapping, the returned data will be wrapped. See [Notes About Union Data Types](#notes-about-union-data-types) below.

## Notes About Union Data Types
To quote the [Avro spec](http://avro.apache.org/docs/current/spec.html#Unions):

*Unions may not contain more than one schema with the same type, except for the named types record, fixed and enum. For example, unions containing two array types or two map types are not permitted, but two types with different names are permitted.*

In Lancaster, the data for both `records` and `maps` can be Clojure hash-maps.
Also, multiple record schemas can be included in be a union. This makes it
impossible, (or at least difficult) to determine which member schema of a union
to use at serialization time.

A Lancaster union schema is *ambiguous* if it contains:
* More than one `record` or `map`
* More than one numeric type: `int`, `long`, `float`, or `double`
* More than one bytes type: `bytes`, `string`, or `fixed`.

Given a Clojure hash-map, and an ambiguous union schema, which schema should
be used to serialize it? This is resolved via *wrapping*.

Wrapping indicates the schema of the given data. This is easily done with the
[wrap](#wrap) function.

### Example
```clojure
;; Define two record schemas
(l/def-record-schema person-schema
  [:name l/string-schema "No name"]
  [:age l/int-schema 0])

(l/def-record-schema dog-schema
  [:name l/string-schema]
  [:owner l/string-schema])

;; Define an ambiguous union schema, using the two records
(l/def-union-schema person-or-dog-schema
  person-schema dog-schema)

(def fido {:name "Fido" :owner "Roger"})

;; Serializing without wrapping fails because the union is ambiguous:
(l/serialize person-or-dog-schema fido)
;; ExceptionInfo Union requires wrapping, but data is not wrapped.

;; Wrapping the data before serialization tells the union which type
;; to use when serializing.
(def wrapped-fido (l/wrap dog-schema fido))
;; {:dog {:name "Fido" :owner "Roger"}}

;; This works now
(l/serialize person-or-dog-schema wrapped-fido)
;; #object["[B" 0x2cc2072e "[B@2cc2072e"]
```

# Names and Namespaces
Named Avro schemas (`records`, `enums`, and `fixeds`) contain a name
part and, optionally, a namespace part.
The
[Names section of the Avro spec](#http://avro.apache.org/docs/current/spec.html#names)
describes this in detail. Lancaster fully supports the spec, allowing
both names and namespaces. These are combined into a single fullname,
including both the namespace (if any) and the name.

Lancaster schema names must start with a letter and subsequently
only contain letters, numbers, or hyphens.

When using the [Schema Creation Macros](#schema-creation-macros),
the name used in the schema is derived from the name of the symbol
passed to the `def-*-schema` macro. It the symbol ends with -schema
(as is common), the -schema portion is dropped from the name. The namespace is
taken from the Clojure(Script) namespace where the schema is defined.

When using the [Schema Creation Functions](#schema-creation-functions),
the name and namespace are taken from the `name-kw` parameter passed to
the `*-schema` function. If the keyword is namespaced, the keyword's namespace
is used as the schema's namespace. If the keyword does not have
a namespace, the schema will not have a namespace.
Only the functions for creating named schemas
([enum-schema](#enum-schema), [fixed-schema](#fixed-schema),
[record-schema](#record-schema), [flex-map-schema](#flex-map-schema),
and [merged-record-schema](#merged-record-schema)) have a `name-kw` parameter.

In the EDN representation of a named schema, the :name attribute
contains the name of the schema, including the namespace, if any.
In the JSON and PCF representations, the name is also converted
from kebab-case to PascalCase, and any namespace is converted from
kebab-case to snake_case. This matches the Avro spec (which does not allow
hyphenated names) and provides easy interop with other languages
(Java, JS, C++, etc.)

For example, using the [def-enum-schema](#def-enum-schema) macro:

```clojure
(l/def-enum-schema suite-schema
  :clubs :diamonds :hearts :spades)

(l/edn suite-schema)
;; {:name :user/suite, :type :enum, :symbols [:clubs :diamonds :hearts :spades]}
;; Note that the :name includes the namespace (:user in this case)
;; and that the name is 'suite', not 'suite-schema'

(l/json suite-schema)
;; "{\"name\":\"user.Suite\",\"type\":\"enum\",\"symbols\":[\"CLUBS\",\"DIAMONDS\",\"HEARTS\",\"SPADES\"]}"
;; Note that the name has been converted to user.Suite

```

Or using the [enum-schema](#enum-schema) function:
```clojure
(def suite-schema
  (l/enum-schema :a-random-ns/suite [:clubs :diamonds :hearts :spades]))

(l/edn suite-schema)
;; {:name :a-random-ns/suite, :type :enum, :symbols [:clubs :diamonds :hearts :spades]}
;; Note that the namespace is not :user, but is :a-random-ns

(l/json suite-schema)
;; "{\"name\":\"a_random_ns.Suite\",\"type\":\"enum\",\"symbols\":[\"CLUBS\",\"DIAMONDS\",\"HEARTS\",\"SPADES\"]}"
;; Note that the name has been converted to a_random_ns.Suite
```


# API Documentation
All public vars, functions, and macros are in the `deercreeklabs.lancaster`
namespace. All other namespaces should be considered private implementation
details that may change.

-------------------------------------------------------------------------------
### def-record-schema
```clojure
(def-record-schema name-symbol & fields)
```
Defines a var whose value is a Lancaster schema object representing an Avro
[```record```](http://avro.apache.org/docs/current/spec.html#schema_record).
For cases where a macro is not appropriate, use the
[record-schema](#record-schema) function instead.

#### Parameters:
* `name-symbol`: The symbol naming this schema object. The Avro schema name
is also derived from this symbol. See
[Names and Namespaces](#names-and-namespaces) for more information about
schema names. The name-symbol must start with a letter and subsequently
only contain letters, numbers, or hyphens.
* `fields`: Field definitions. Field definitions are sequences
            of the form ```[field-name-kw field-schema default-value]```.
    * `field-name-kw`: A keyword naming this field.
    * `field-schema`: A Lancaster schema object representing the field's schema.
    * `default-value`: Optional. The default data value for this field.

#### Return Value:
The defined var

#### Example
```clojure
(l/def-record-schema person-schema
  [:name l/string-schema "no name"]
  [:age l/int-schema])
```

#### See Also:
* [record-schema](#record-schema) Creates a record schema.

-------------------------------------------------------------------------------
### def-merged-record-schema
```clojure
(def-merged-record-schema name-symbol & record-schemas)
```
Defines a var whose value is a Lancaster schema object representing an Avro
[```record```](http://avro.apache.org/docs/current/spec.html#schema_record).
The record schema contains all the fields of all record schemas passed in.
For cases where a macro is not appropriate, use the
[merged-record-schema](#merged-record-schema) function instead.

#### Parameters:
* `name-symbol`: The symbol naming this schema object. The Avro schema name
is also derived from this symbol. See
[Names and Namespaces](#names-and-namespaces) for more information about
schema names. The name-symbol must start with a letter and subsequently
only contain letters, numbers, or hyphens.
* `record-schemas`: Lancaster schema record objects to be merged.

#### Return Value:
The defined var

#### Example
```clojure
(l/def-record-schema person-schema
  [:name l/string-schema]
  [:age l/int-schema])

(l/def-record-schema location-schema
  [:latitude l/double-schema]
  [:longitude l/double-schema])

(l/def-merged-record-schema person-w-lat-long-schema
  person-schema location-schema)
```

#### See Also:
* [merged-record-schema](merged-record-schema) Creates a record schema which contains all the fields of all record schemas passed in.
* [record-schema](#record-schema) Creates a record schema.

-------------------------------------------------------------------------------
### def-enum-schema
```clojure
(def-enum-schema name-symbol & symbol-keywords)
```
Defines a var whose value is a Lancaster schema object representing an Avro
[```enum```](http://avro.apache.org/docs/current/spec.html#Enums).
For cases where a macro is not appropriate, use the
[enum-schema](#enum-schema) function instead.

#### Parameters:
* `name-symbol`: The symbol naming this schema object. The Avro schema name
is also derived from this symbol. See
[Names and Namespaces](#names-and-namespaces) for more information about
schema names. The name-symbol must start with a letter and subsequently
only contain letters, numbers, or hyphens.
* `symbol-keywords`: Keywords representing the symbols in the enum.

#### Return Value:
The defined var

#### Example
```clojure
(l/def-enum-schema suite-schema
  :clubs :diamonds :hearts :spades)
```

#### See Also:
* [enum-schema](#enum-schema) Creates an enum schema.

-------------------------------------------------------------------------------
### def-fixed-schema
```clojure
(def-fixed-schema name-symbol size)
```
Defines a var whose value is a Lancaster schema object representing an Avro
[```fixed```](http://avro.apache.org/docs/current/spec.html#Fixed).
For cases where a macro is not appropriate, use the
[fixed-schema](#fixed-schema) function instead.

#### Parameters:
* `name-symbol`: The symbol naming this schema object. The Avro schema name
is also derived from this symbol. See
[Names and Namespaces](#names-and-namespaces) for more information about
schema names. The name-symbol must start with a letter and subsequently
only contain letters, numbers, or hyphens.
* `size`: An integer representing the size of this fixed in bytes.

#### Return Value:
The defined var

#### Example
```clojure
(l/def-fixed-scheema md5-schema
  16)
```

#### See Also:
* [fixed-schema](#fixed-schema) Creates a fixed schema.

-------------------------------------------------------------------------------
### def-array-schema
```clojure
(def-array-schema name-symbol items-schema)
```
Defines a var whose value is a Lancaster schema object representing an Avro
[```array```](http://avro.apache.org/docs/current/spec.html#Arrays).
For cases where a macro is not appropriate, use the
[array-schema](#array-schema) function instead.

#### Parameters:
* `name-symbol`: The symbol naming this schema object.
* `items-schema`: A Lancaster schema object describing the items in the array.

#### Return Value:
The defined var

#### Example
```clojure
(l/def-array-scheema numbers-schema
  l/int-schema)
```

#### See Also:
* [array-schema](#array-schema) Creates an array schema.

-------------------------------------------------------------------------------
### def-map-schema
```clojure
(def-map-schema name-symbol values-schema)
```
Defines a var whose value is a Lancaster schema object representing an Avro
[```map```](http://avro.apache.org/docs/current/spec.html#Maps).
For cases where a macro is not appropriate, use the
[map-schema](#map-schema) function instead.

#### Parameters:
* `name-symbol`: The symbol naming this schema object.
* `values-schema`: A Lancaster schema object describing the values in the map.
Map keys are always strings.

#### Return Value:
The defined var

#### Example
```clojure
(l/def-map-scheema name-to-age-schema
  l/int-schema)
```

#### See Also:
* [map-schema](#map-schema) Creates a map schema.

-------------------------------------------------------------------------------
### def-flex-map-schema
```clojure
(def-flex-map-schema name-symbol keys-schema values-schema)
```
Defines a var whose value is a Lancaster schema object representing a
map of keys to values, with the keys and values being described by the
given schemas. Differs from def-map-schema, which only allows string keys.
Note that flex-maps are not part of the
[Avro Specification](http://avro.apache.org/docs/current/spec.html)
and are implemented using an Avro `record`.
For cases where a macro is not appropriate, use the
[flex-map-schema](#flex-map-schema) function instead.

#### Parameters:
* `name-symbol`: The symbol naming this schema object.
* `keys-schema`: A Lancaster schema object describing the keys in the map.
* `values-schema`: A Lancaster schema object describing the values in the map.

#### Return Value:
The defined var

#### Example
```clojure
(l/def-flex-map-scheema id-to-name-schema
  l/int-schema l/string-schema)
```

#### See Also:
* [flex-map-schema](#flex-map-schema) Creates a flex-map schema.

-------------------------------------------------------------------------------
### def-union-schema
```clojure
(def-union-schema name-symbol & member-schemas)
```
Defines a var whose value is a Lancaster schema object representing an Avro
[```union```](http://avro.apache.org/docs/current/spec.html#Unions).
For cases where a macro is not appropriate, use the
[union-schema](#union-schema) function instead.

#### Parameters:
* `name-symbol`: The symbol naming this schema object.
* `member-schemas`: Lancaster schema objects representing the members of the
union.

#### Return Value:
The defined var

#### Example
```clojure
(l/def-union-scheema maybe-name-schema
  l/null-schema l/string-schema)
```

#### See Also:
* [union-schema](#union-schema) Creates a union schema.

-------------------------------------------------------------------------------
### def-maybe-schema
```clojure
(def-maybe-schema name-symbol schemas)
```
Defines a var whose value is a Lancaster schema object representing an Avro
[```union```](http://avro.apache.org/docs/current/spec.html#Unions). The
members of the union are null-schema and the given schema. Makes a
schema nillable. For cases where a macro is not appropriate, use the
[maybe](#maybe) function instead.

#### Parameters:
* `name-symbol`: The symbol naming this schema object.
* `schema`: Lancaster schema object representing the non-nil member
of the union.

#### Return Value:
The defined var

#### Example
```clojure
(l/def-maybe-scheema maybe-name-schema
  l/string-schema)
```

#### See Also:
* [maybe](#maybe) Creates a nillable schema.

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

#### See Also:
* [def-record-schema](#def-record-schema) Defines a var w/ a record schema.

-------------------------------------------------------------------------------
### enum-schema
```clojure
(enum-schema name-kw symbol-keywords)
```
Creates a Lancaster schema object representing an Avro
[```enum```](http://avro.apache.org/docs/current/spec.html#Enums),
with the given name and keyword symbols. For a more
concise way to declare an enum schema, see
[def-enum-schema](#def-enum-schema).

#### Parameters:
* `name-kw`: A keyword naming this ```enum```. May or may not be
             namespaced. The name-kw must start with a letter and subsequently
             only contain letters, numbers, or hyphens.
* `symbol-keywords`: A sequence of keywords, representing the symbols in
             the enum.

#### Return Value:
The new Lancaster enum schema.

#### Example
```clojure
(def suite-schema
  (l/enum-schema :suite [:clubs :diamonds :hearts :spades]))
```

#### See Also
* [def-enum-schema](#def-enum-schema) Defines a var w/ an enum schema.

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

#### See Also
* [def-fixed-schema](#def-fixed-schema) Defines a var w/ a fixed schema.

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

#### See Also
* [def-array-schema](#def-array-schema) Defines a var w/ an array schema.

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
(def name-to-age-schema (l/map-schema l/int-schema))
```

#### See Also
* [def-map-schema](#def-map-schema) Defines a var w/ a map schema.
* [flex-map-schema](#flex-map-schema) Creates a flex-map schema. Flex map keys may be of any schema type.
* [def-flex-map-schema](#def-flex-map-schema) Defines a var w/ a flex-map schema. Flex map keys may be of any schema type.

-------------------------------------------------------------------------------
### flex-map-schema
```clojure
(flex-map-schema keys-schema values-schema)
```
Creates a Lancaster schema object representing a
map of keys to values, with the keys and values being described by the
given schemas. Differs from map-schema, which only allows string keys.
Note that flex-maps are not part of the
[Avro Specification](http://avro.apache.org/docs/current/spec.html)
and are implemented using an Avro `record`.

#### Parameters:
* `keys-schema`: A Lancaster schema object describing the keys in the map.
* `values-schema`: A Lancaster schema object describing the values in the map.

#### Return Value:
The new Lancaster flex-map schema.

#### Examples
```clojure
(def id-to-name-schema
  (l/flex-map-schema l/int-schema l/string-schema))
```
#### See Also
* [def-flex-map-schema](#def-flex-map-schema) Defines a var w/ a flex-map schema. Flex map keys may be of any schema type.
* [map-schema](#map-schema) Creates a map schema. Keys must be strings.

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

#### See Also
* [def-union-schema](#def-union-schema) Defines a var w/ a union schema.

-------------------------------------------------------------------------------
### merged-record-schema
```clojure
(merged-record-schema name-kw schemas)
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
  (l/merged-record-schema [person-schema location-schema]))
```

#### See also
* [def-merged-record-schema](#def-merged-record-schema) Defines a var w/ a record schema which contains all the fields of all record schemas passed in.
* [record-schema](#record-schema) Creates a record schema.

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

#### See also
* [def-maybe-schema](#def-maybe-schema) Defines a var w/ a nillable schema.

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
#### See also
* [deserialize](#deserialize) Deserializes data from a byte array, using separate reader and writer schemas. **This is the recommended deserialization function**.
* [deserialize-same](#deserialize-) Deserializes data from a byte array, using the same reader and writer schema. **This is not recommended**, as it does not allow for [schema

-------------------------------------------------------------------------------
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
#### See also
* [serialize](#serialize) Serializes data to a byte array.
* [deserialize-same](#deserialize-) Deserializes data from a byte array, using the same reader and writer schema. **This is not recommended**, as it does not allow for [schema
resolution / evolution](http://avro.apache.org/docs/current/spec.html#Schema+Resolution).

-------------------------------------------------------------------------------
### deserialize-same
```clojure
(deserialize-same schema ba)
```
Deserializes Avro-encoded data from a byte array, using the given schema
as both the reader and writer schema.

**Note that this is not recommended**, since it does not allow for [schema
resolution / evolution](http://avro.apache.org/docs/current/spec.html#Schema+Resolution). The original writer's schema
should always be used to deserialize. The writer's schema
(in [Parsing Canonical Form](http://avro.apache.org/docs/current/spec.html#Parsing+Canonical+Form+for+Schemas))
should always be stored or transmitted with encoded data. If the schema specified
in this function does not match the schema with which the data was encoded,
the function will fail, possibly in strange ways. You should generally use
the [deserialize](#deserialize) function instead.

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

#### See also
* [serialize](#serialize) Serializes data to a byte array.
* [deserialize](#deserialize) Deserializes data from a byte array, using separate reader and writer schemas. **This is the recommended deserialization function**.

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
[Notes About Union Data Types](#notes-about-union-data-types)
for more information.

#### Parameters:
* `data-schema`: The Lancaster schema of the data to be wrapped
* `data`: The data to be wrapped.

#### Return Value
The wrapped data.

#### Example
```clojure
;; Define two record schemas
(l/def-record-schema person-schema
  [:name l/string-schema "No name"]
  [:age l/int-schema 0])

(l/def-record-schema dog-schema
  [:name l/string-schema]
  [:owner l/string-schema])

;; Define an ambiguous union schema, using the two records
(l/def-union-schema person-or-dog-schema
  person-schema dog-schema)

(def fido {:name "Fido" :owner "Roger"})

;; Serializing without wrapping fails because the union is ambiguous:
(l/serialize person-or-dog-schema fido)
;; ExceptionInfo Union requires wrapping, but data is not wrapped.

;; Wrapping the data before serialization tells the union which type
;; to use when serializing.
(def wrapped-fido (l/wrap dog-schema fido))
;; {:dog {:name "Fido" :owner "Roger"}}

;; This works now
(l/serialize person-or-dog-schema wrapped-fido)
;; #object["[B" 0x2cc2072e "[B@2cc2072e"]
```

-------------------------------------------------------------------------------
### edn
```clojure
(edn schema)
```
Returns the EDN representation of the given Lancaster schema.

#### Parameters:
* `schema`: The Lancaster schema

#### Return Value
EDN representation of the given Lancaster schema

#### Example
```clojure
(l/def-enum-schema suite-schema
  :clubs :diamonds :hearts :spades)

(l/edn suite-schema)
;; {:name :suite, :type :enum, :symbols [:clubs :diamonds :hearts :spades]}
```

#### See Also
* [json](#json) Returns the JSON representation of the schema

-------------------------------------------------------------------------------
### json
```clojure
(json schema)
```
Returns an Avro-compliant JSON representation of the given Lancaster schema.

#### Parameters:
* `schema`: The Lancaster schema

#### Return Value
JSON representation of the given Lancaster schema

#### Example
```clojure
(l/def-enum-schema suite-schema
  :clubs :diamonds :hearts :spades)

(l/json suite-schema)
;; "{\"name\":\"Suite\",\"type\":\"enum\",\"symbols\":[\"CLUBS\",\"DIAMONDS\",\"HEARTS\",\"SPADES\"]}"
```

#### See Also
* [edn](#edn) Returns the EDN representation of the schema

-------------------------------------------------------------------------------
### plumatic-schema
```clojure
(plumatic-schema schema)
```
Returns a [Plumatic schema](https://github.com/plumatic/schema)
for the given Lancaster schema

#### Parameters:
* `schema`: The Lancaster schema

#### Return Value
A Plumatic schema that matches the Lancaster schema

#### Example
```clojure
(l/def-enum-schema suite-schema
  :clubs :diamonds :hearts :spades)

(l/plumatic-schema suite-schema)
;; (enum :spades :diamonds :clubs :hearts)
```

-------------------------------------------------------------------------------
### pcf
```clojure
(pcf schema)
```
Returns a JSON string containing the
[Parsing Canonical Form](http://avro.apache.org/docs/current/spec.html#Parsing+Canonical+Form+for+Schemas)
for the given Lancaster schema.

#### Parameters:
* `schema`: The Lancaster schema

#### Return Value
A JSON string

#### Example
```clojure
(l/def-enum-schema suite-schema
  :clubs :diamonds :hearts :spades)

(l/pcf suite-schema)
;; "{\"name\":\"Suite\",\"type\":\"enum\",\"symbols\":[\"CLUBS\",\"DIAMONDS\",\"HEARTS\",\"SPADES\"]}"
;; Note that this happens to be the same as (l/json suite-schema) for this
;; particular schema. That is not generally the case.
```

#### See Also
* [fingerprint64](#fingerprint64) Returns the 64-bit [Rabin fingerprint](http://en.wikipedia.org/wiki/Rabin_fingerprint) of the [Parsing Canonical Form](http://avro.apache.org/docs/current/spec.html#Parsing+Canonical+Form+for+Schemas) of the schema.

-------------------------------------------------------------------------------
### fingerprint64
```clojure
(fingerprint64 schema)
```
Returns the 64-bit
[Rabin fingerprint](http://en.wikipedia.org/wiki/Rabin_fingerprint) of the
[Parsing Canonical Form](http://avro.apache.org/docs/current/spec.html#Parsing+Canonical+Form+for+Schemas)
for the given Lancaster schema.

#### Parameters:
* `schema`: The Lancaster schema

#### Return Value
A 64-bit Long representing the fingerprint. For JVM Clojure, this is a
java.lang.Long. For ClojureScript, it is a goog.math.Long.

#### Example
```clojure
(l/def-enum-schema suite-schema
  :clubs :diamonds :hearts :spades)

(l/fingerprint64 suite-schema)
;; 5882396032713186004
```

#### See Also
* [pcf](#pcf) Returns a JSON string containing the
[Parsing Canonical Form](http://avro.apache.org/docs/current/spec.html#Parsing+Canonical+Form+for+Schemas) of the schema

-------------------------------------------------------------------------------
### schema?
```clojure
(schema? arg)
```
Returns a boolean indicating whether or not the argument is a
Lancaster schema object.

#### Parameters:
* `arg`: The argument to be tested

#### Return Value
A boolean indicating whether or not the argument is a
Lancaster schema object

#### Example
```clojure
(l/def-enum-schema suite-schema
  :clubs :diamonds :hearts :spades)

(l/schema? suite-schema)
;; true

(l/schema? :clubs)
;; false
```

-------------------------------------------------------------------------------
### default-data
```clojure
(default-data schema)
```
Creates default data that conforms to the given Lancaster schema. The following
values are used for the primitive data types:
* `null`: `nil`
* `boolean`: `false`
* `int`: `-1`
* `long`: `-1`
* `float`: `-1.0`
* `double`: `-1.0`
* `string`: `""`
* `enum`: first symbol in the schema's symbols list

Default data for complex schemas are built up from the primitives.

#### Parameters:
* `schema`: The Lancaster schema

#### Return Value
Data that matches the given schema

#### Example
```clojure
(l/def-enum-schema suite-schema
  :clubs :diamonds :hearts :spades)

(l/default-data suite-schema)
;; :clubs
```

# License

Copyright (c) 2017-2019 Deer Creek Labs, LLC

*Apache Avro, Avro, Apache, and the Avro and Apache logos are trademarks of The Apache Software Foundation.*

Distributed under the Apache Software License, Version 2.0
http://www.apache.org/licenses/LICENSE-2.0.txt
