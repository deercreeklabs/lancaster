(ns deercreeklabs.unit.evolution-test
  (:require
   [clojure.string :as str]
   [clojure.test :refer [deftest is use-fixtures]]
   [deercreeklabs.baracus :as ba]
   [deercreeklabs.lancaster :as l]
   [deercreeklabs.lancaster.utils :as u]
   [deercreeklabs.unit.lancaster-test :as lt]
   [schema.core :as s :include-macros true])
  #?(:clj
     (:import
      (clojure.lang ExceptionInfo))))

(deftest test-record-schema-evolution-add-field
  (let [data {:sku 789
              :qty-requested 10}
        writer-schema lt/add-to-cart-req-schema
        reader-schema lt/add-to-cart-req-v2-schema
        encoded (l/serialize writer-schema data)
        decoded (l/deserialize reader-schema writer-schema encoded)]
    (is (= data decoded))))

(deftest test-schema-evolution-remove-field
  (let [data {:sku 789
              :qty-requested 10
              :note "This is a nice item"}
        writer-schema lt/add-to-cart-req-v2-schema
        reader-schema lt/add-to-cart-req-schema
        encoded (l/serialize writer-schema data)
        decoded (l/deserialize reader-schema writer-schema encoded)]
    (is (= (dissoc data :note) decoded))))

(deftest test-schema-evolution-change-field
  (let [data {:sku 123
              :qty-requested 10
              :note "This is a nice item"}
        writer-schema lt/add-to-cart-req-v2-schema
        reader-schema lt/add-to-cart-req-v3-schema
        encoded (l/serialize writer-schema data)
        decoded (l/deserialize reader-schema writer-schema encoded)]
    (is (= (assoc data :qty-requested 10.0) decoded))))

(deftest test-schema-evolution-add-and-delete-field
  (let [data {:sku 123
              :qty-requested 10
              :note "This is a nice item"}
        writer-schema lt/add-to-cart-req-v2-schema
        reader-schema lt/add-to-cart-req-v4-schema
        encoded (l/serialize writer-schema data)
        decoded (l/deserialize reader-schema writer-schema encoded)
        expected (dissoc data :note)]
    (is (= expected decoded))))

(deftest test-schema-evolution-add-field-and-change-field
  (let [data {:sku 123
              :qty-requested 10}
        writer-schema lt/add-to-cart-req-schema
        reader-schema lt/add-to-cart-req-v3-schema
        encoded (l/serialize writer-schema data)
        decoded (l/deserialize reader-schema writer-schema encoded)]
    (is (= (assoc data
                  :qty-requested 10.0)
           decoded))))

(deftest test-schema-evolution-union-add-member
  (let [data #:dog{:name "Rover" :owner "Zeus"}
        writer-schema lt/person-or-dog-schema
        reader-schema lt/fish-or-person-or-dog-v2-schema
        encoded (l/serialize writer-schema data)
        decoded (l/deserialize reader-schema writer-schema encoded)]
    (is (= data decoded))))

(deftest test-schema-evolution-union-to-non-union
  (let [data #:dog{:name "Rover" :owner "Zeus"}
        writer-schema lt/person-or-dog-schema
        reader-schema lt/dog-v2-schema
        encoded (l/serialize writer-schema data)
        decoded (l/deserialize reader-schema writer-schema encoded)]
    (is (= data decoded))))

(deftest test-schema-evolution-non-union-to-union
  (let [data #:dog{:name "Rover" :owner "Zeus" :tag-number 123}
        writer-schema lt/dog-v2-schema
        reader-schema lt/person-or-dog-schema
        encoded (l/serialize writer-schema data)
        decoded (l/deserialize reader-schema writer-schema encoded)
        expected (dissoc data :dog/tag-number)]
    (is (= expected decoded))))

(deftest test-schema-evolution-union-remove-member-success
  (let [data #:dog{:name "Runner" :owner "Tommy" :tag-number 134}
        writer-schema lt/fish-or-person-or-dog-v2-schema
        reader-schema lt/person-or-dog-schema
        encoded (l/serialize writer-schema data)
        decoded (l/deserialize reader-schema writer-schema encoded)
        expected (dissoc data :dog/tag-number)]
    (is (= expected decoded))))

(deftest test-schema-evolution-union-remove-member-failure
  (let [data {:fish/name "Swimmy" :tank-num 24}
        writer-schema lt/fish-or-person-or-dog-v2-schema
        reader-schema lt/person-or-dog-schema
        encoded (l/serialize writer-schema data)]
    (try
      (l/deserialize reader-schema writer-schema encoded)
      (is (= :did-not-throw :but-should-have))
      (catch #?(:clj Exception :cljs js/Error) e
        (let [msg (u/ex-msg e)]
          (is (str/includes? msg "does not match")))))))

(deftest test-schema-evolution-no-match
  (let [data {:sku 123
              :qty-requested 10}
        writer-schema lt/add-to-cart-req-schema
        reader-schema l/int-schema
        encoded (l/serialize writer-schema data)]
    (is (thrown-with-msg?
         #?(:clj ExceptionInfo :cljs js/Error)
         #"does not match."
         (l/deserialize reader-schema writer-schema encoded)))))

(deftest test-schema-evolution-named-ref
  (let [data {:game/players [{:name/first "Chad" :name/last "Harrington"}]
              :game/judges [{:name/first "Chibuzor" :name/last "Okonkwo"}]}
        name-schema (l/record-schema
                     ::lt/name
                     [[:name/first l/string-schema]
                      [:name/last l/string-schema]])
        writer-schema (l/record-schema
                       ::lt/game
                       [[:game/players (l/array-schema name-schema)]
                        [:game/judges (l/array-schema name-schema)]])
        reader-schema (l/record-schema
                       ::lt/game
                       [[:game/players (l/array-schema name-schema)]
                        [:game/judges (l/array-schema name-schema)]
                        [:game/audience (l/array-schema name-schema)]])
        encoded (l/serialize writer-schema data)
        decoded (l/deserialize reader-schema writer-schema encoded)]
    (is (= (str
            "{\"name\":\"deercreeklabs.unit.lancaster_test.Game\",\"type\":"
            "\"record\",\"fields\":[{\"name\":\"gamePlayers\",\"type\":"
            "[\"null\",{\"type\":\"array\",\"items\":{\"name\":\"deercreeklabs"
            ".unit.lancaster_test.Name\",\"type\":\"record\",\"fields\":"
            "[{\"name\":\"nameFirst\",\"type\":[\"null\",\"string\"]},{\"name\""
            ":\"nameLast\",\"type\":[\"null\",\"string\"]}]}}]},{\"name\":"
            "\"gameJudges\",\"type\":[\"null\",{\"type\":\"array\",\"items\":"
            "\"deercreeklabs.unit.lancaster_test.Name\"}]}]}")
           (l/pcf writer-schema)))
    (is (= data decoded))))

(deftest test-schema-evolution-int-to-long
  (let [data 10
        writer-schema l/int-schema
        reader-schema l/long-schema
        encoded-orig (l/serialize writer-schema data)
        decoded-new (l/deserialize reader-schema writer-schema encoded-orig)]
    (is (= data decoded-new))))

(deftest test-schema-evolution-int-to-float
(let [data 10
      writer-schema l/int-schema
      reader-schema l/float-schema
      encoded-orig (l/serialize writer-schema data)
      decoded-new (l/deserialize reader-schema writer-schema encoded-orig)]
  (is (= (float data) decoded-new))))

(deftest test-schema-evolution-int-to-double
(let [data 10
      writer-schema l/int-schema
      reader-schema l/float-schema
      encoded-orig (l/serialize writer-schema data)
      decoded-new (l/deserialize reader-schema writer-schema encoded-orig)]
  (is (= (double data) decoded-new))))

(deftest test-schema-evolution-long-to-float
(let [data (u/ints->long 12345 6789)
      writer-schema l/long-schema
      reader-schema l/float-schema
      encoded-orig (l/serialize writer-schema data)
      decoded (l/deserialize reader-schema writer-schema encoded-orig)
      expected 5.3021371E13
      rel-err (lt/rel-err expected decoded)]
  (is (> 0.00000001 rel-err))))

(deftest test-schema-evolution-long-to-double
(let [data (u/ints->long -12345 -6789)
      writer-schema l/long-schema
      reader-schema l/double-schema
      encoded-orig (l/serialize writer-schema data)
      decoded (l/deserialize reader-schema writer-schema encoded-orig)
      expected (double -53017076308613)
      rel-err (lt/rel-err expected decoded)]
  (is (> 0.00000001 rel-err))))

(deftest test-schema-evolution-float-to-double
(let [data (float 1234.5789)
      writer-schema l/float-schema
      reader-schema l/double-schema
      encoded-orig (l/serialize writer-schema data)
      decoded (l/deserialize reader-schema writer-schema encoded-orig)
      rel-err (lt/rel-err data decoded)]
  (is (> 0.0000001 rel-err))))

(deftest test-schema-evolution-string-to-bytes
(let [data "Hello, World!"
      writer-schema l/string-schema
      reader-schema l/bytes-schema
      encoded (l/serialize writer-schema data)
      decoded (l/deserialize reader-schema writer-schema encoded)
      expected (ba/byte-array [72 101 108 108 111 44
                               32 87 111 114 108 100 33])]
  (is (ba/equivalent-byte-arrays? expected decoded))))

(deftest test-schema-evolution-int-array-to-float-array
  (let [data [1 2 3]
        writer-schema (l/array-schema l/int-schema)
        reader-schema (l/array-schema l/float-schema)
        encoded (l/serialize writer-schema data)
        decoded (l/deserialize reader-schema writer-schema encoded)
        expected [1.0 2.0 3.0]]
    (is (= expected decoded))))

(deftest test-schema-evolution-int-map-to-float-map
  (let [data {"one" 1 "two" 2}
        writer-schema (l/map-schema l/int-schema)
        reader-schema (l/map-schema l/float-schema)
        encoded (l/serialize writer-schema data)
        decoded (l/deserialize reader-schema writer-schema encoded)
        expected {"one" 1.0 "two" 2.0}]
    (is (= expected decoded))))

(deftest  test-schema-evolution-enum-added-symbol
  (let [data :stock
        writer-schema lt/why-schema
        reader-schema (l/enum-schema ::lt/why
                                     [:foo :all :limit :stock])
        encoded (l/serialize writer-schema data)
        decoded (l/deserialize reader-schema writer-schema encoded)]
    (is (= data decoded))))

(deftest test-union-evolution-1
  (let [union-a-schema (l/union-schema [l/string-schema l/int-schema])
        union-b-schema (l/union-schema [l/int-schema l/string-schema])
        rt #(->> (l/serialize union-a-schema %)
                 (l/deserialize union-b-schema union-a-schema))
        data-1 1
        data-a "a"]
    (is (= data-1 (rt data-1)))
    (is (= data-a (rt data-a)))))

(deftest test-union-evolution-2
  (let [union-a-schema (l/union-schema [l/string-schema l/int-schema])
        union-b-schema (l/union-schema [l/int-schema l/string-schema])
        rt #(->> (l/serialize union-b-schema %)
                 (l/deserialize union-a-schema union-b-schema))
        data-1 1
        data-a "a"]
    (is (= data-1 (rt data-1)))
    (is (= data-a (rt data-a)))))

(deftest test-union-evolution-inside-rec
  (let [union-a-schema (l/union-schema [l/string-schema l/int-schema])
        union-b-schema (l/union-schema [l/int-schema l/string-schema])
        w-schema (l/record-schema ::rec
                                  [[:v union-a-schema]])
        r-schema (l/record-schema ::rec
                                  [[:v union-b-schema]])
        data {:v 1}
        encoded (l/serialize w-schema data)
        decoded (l/deserialize r-schema w-schema encoded)]
    (is (= data decoded))))

(deftest test-enum-evolution-default
  (let [w-schema (l/enum-schema ::test [:a :b :c :d])
        r-schema (l/enum-schema ::test [:a :b :c])
        data :d
        encoded (l/serialize w-schema data)
        decoded (l/deserialize r-schema w-schema encoded)]
    (is (= :a decoded))))

(deftest test-record-evolution-ns-added-field
  (let [w-schema (l/record-schema ::test [[:user/name l/string-schema]])
        r-schema (l/record-schema ::test [[:user/name l/string-schema]
                                          [:user/age l/int-schema]])
        data {:user/name "Alice"}
        encoded (l/serialize w-schema data)
        decoded (l/deserialize r-schema w-schema encoded)
        expected {:user/name "Alice"}]
    (is (= expected decoded))))

(deftest test-record-evolution-ns-deleted-field
  (let [w-schema (l/record-schema ::test [[:user/name l/string-schema]
                                          [:user/age l/int-schema]])
        r-schema (l/record-schema ::test [[:user/name l/string-schema]])
        data {:user/name "Alice"
              :user/age 40}
        encoded (l/serialize w-schema data)
        decoded (l/deserialize r-schema w-schema encoded)
        expected {:user/name "Alice"}]
    (is (= expected decoded))))

(deftest test-nested-kw-evolution
  (let [unit-schema (l/enum-schema ::unit [:a :b])
        w-schema (l/record-schema ::foo
                                  [[:name l/string-schema]
                                   [:unit-1 unit-schema]
                                   [:unit-2 unit-schema]])
        r-schema (l/record-schema ::foo
                                  [[:name l/string-schema]])
        data {:name "blah"
              :unit-1 :a
              :unit-2 :b}
        encoded (l/serialize w-schema data)
        decoded (l/deserialize r-schema w-schema encoded)
        expected (dissoc data :unit-1 :unit-2)]
    (is (= expected decoded))))
