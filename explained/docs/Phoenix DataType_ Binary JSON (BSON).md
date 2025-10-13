# Phoenix DataType: Binary JSON (BSON)

**Author: [Viraj Jasani](mailto:virajjasani007@gmail.com)**

### **Jira: [PHOENIX-7330](https://issues.apache.org/jira/browse/PHOENIX-7330)**

This document provides an introduction of a new datatype in Phoenix: BSON. BSON or Binary JSON is a Binary-Encoded serialization of JSON-like documents. BSON data type is specifically used for users to store, update and query part or whole of the BsonDocument in the most performant way without having to serialize/deserialize the document to/from binary format. Bson allows deserializing only part of the nested documents such that querying or indexing any attributes within the nested structure becomes more efficient and performant as the deserialization happens at runtime. Any other document structure would require deserializing the binary into the document, and then perform the query.

BSONSpec: [https://bsonspec.org/](https://bsonspec.org/)

### **JSON vs BSON and why do we need BSON?**

JSON and BSON are closely related by design. BSON serves as a binary representation of JSON data, tailored with specialized extensions for wider application scenarios, and finely tuned for efficient data storage and traversal. Similar to JSON, BSON facilitates the embedding of objects and arrays.

One particular way in which BSON differs from JSON is in its support for some more advanced data types. For instance, JSON does not differentiate between integers (round numbers), and floating-point numbers (with decimal precision). BSON does distinguish between the two and store them in the corresponding BSON data type (e.g. BsonInt32 vs BsonDouble). Many server-side programming languages offer advanced numeric data types (standards include integer, regular precision floating point number i.e. “float”, double-precision floating point i.e. “double”, and boolean values), each with its own optimal usage for efficient mathematical operations.

Another key distinction between BSON and JSON is that BSON documents have the capability to include Date or Binary objects, which cannot be directly represented in pure JSON format. BSON also provides the ability to store and retrieve user defined Binary objects. Likewise, by integrating advanced data structures like Sets into BSON documents, we can significantly enhance the capabilities of Phoenix for storing, retrieving, and updating Binary, Sets, Lists, and Documents as nested or complex data types.

Moreover, JSON format is human as well as machine readable, whereas BSON format is only machine readable. Hence, as part of introducing BSON data type, we also need to provide a user interface such that users can provide human readable JSON as input for BSON datatype.

Phoenix can introduce more complex data structures like sets of scalar types, in addition to the nested documents and nested arrays provided by BSON.  
Overall, by combining various functionalities available in Phoenix like secondary indexes, conditional updates, high throughput read/write with BSON, we can evolve Phoenix into a highly scalable Document Database.

BSON Scalar Data types:

* String  
* Number  
* Boolean  
* Null  
* Binary  
* Data

BSON Complex Data types:

* Nested List of BSON Values  
* Nested Documents of BSON Values  
* Sets of Scalar BSON types (Set of String, Set of Binary etc)

## **Grammar:**

**DDL Statement Grammar:**

`BSON`

* **Definition:** The BSON-parsable Json to represent the BsonDocument.  
* **Mapped to:** `org.bson.BsonDocument` Java object.

Example:

```c

CREATE TABLE TABLE_NAME (PK1 VARCHAR NOT NULL, PK2 VARCHAR NOT NULL, COL BSON CONSTRAINT pk PRIMARY KEY(PK1, PK2))
```

**DML Statement Grammar:**

```javascript

{
 "<attribute-key>" : <attribute-value>
}
```

* **\<attribute-key\>** represents the String value of attribute keys for the key-value pairs stored in a BSON document.  
* **\<attribute-value\>** represents the attribute value for the key-value pairs stored in a BSON document. Depending on the datatype of the attribute-value, it can be represented in different textual format. The supported data types for the attribute-value are provided here.

#### **\<attribute-value\> datatypes with examples:**

| DataType | Textual format | Examples |
| :---: | ----- | ----- |
| String | sequence of characters | {   "Color": "Gray" } |
| Number | Types of Number format: \- Integer \- Long \- Double \- Decimal | {   "Price": 75295.847,   "Quantity": 10 } |
| Boolean | Boolean values: true or false | {   "ColorPresence": true } |
| Null | Null | {   "Color": null } |
| Binary | Base64 Encoded binary values: "\<binary-attribute-key\>": { "$binary": { "base64": "\<binary-attribute-value\>", "subType": "\<binary-sub-type\>" } } BSON Binary sub-types are provided here: https://bsonspec.org/spec.html | {   "ColorBytes": {     "$binary": {       "base64": "QmxhY2s=",       "subType": "00"     }   } } |
| List | List of attribute-values: "\<list-attribute-key\>": \[ \<comma-separated-list-attribute-values\> \] attribute-values can be of any data type. | {   "Colors": \[     "Blue",     "Red",     123,     true,     null,     "Orange"   \] } |
| Document | Document of attribute keys and values "\<document-attribute-key\>": { "\<nested-attribute-key\>": \<nested-attribute-value\> } nested-attribute-value can be of any data type. | {   "ColorDetails": {     "ColorPresence": true,     "Color": "Blue",     "Quantity": 15   } } |
| Set | Sets of attribute-values As Sets are not directly supported in JSON textual format, define “$set“ datatype: "\<set-attribute-key\>": { "$set": \[ \<comma-separated-set-attribute-values\> \] } set attribute values should be of the same data type. e.g. set of string values or set of binary values etc. | {   "ColorSet": {     "$set": \[       "Blue",       "Red",       "Orange"     \]   } } |
| Date | Date type to support date representation. As Dates are not directly supported in JSON textual format, define "$date" datatype: "\<date-attribute-key\>": { "$date": "\<data-time-format\>" } | {   "PurchaseTime": {     "$date":       "2024-05-01T00:45:25.203Z"   } } |

### 

### **BsonNumber subtypes:**

* [BsonInt32](https://github.com/mongodb/mongo-java-driver/blob/master/bson/src/main/org/bson/BsonInt32.java): 4 bytes (32-bit signed integer, two's complement)  
* [BsonInt64](https://github.com/mongodb/mongo-java-driver/blob/master/bson/src/main/org/bson/BsonInt64.java): 8 bytes (64-bit signed integer, two's complement)  
* [BsonDouble](https://github.com/mongodb/mongo-java-driver/blob/master/bson/src/main/org/bson/BsonDouble.java): 8 bytes (64-bit IEEE 754-2008 binary floating point)  
* [BsonDecimal128](https://github.com/mongodb/mongo-java-driver/blob/master/bson/src/main/org/bson/BsonDecimal128.java): 16 bytes (128-bit IEEE 754-2008 decimal floating point)

#### **UPSERT statement examples:**

```c
UPSERT INTO TABLE_NAME (PK1, PK2, COL) VALUES ('pk011', 'pk012', '{"Price":1234.123,"PurchaseTime":{"$date":"2024-04-01T00:00:20.203Z"}}')
```

```c
UPSERT INTO TABLE_NAME (PK1, PK2, COL) VALUES ('pk001', 'pk002', '{"Title":"Title Value","InPublication":true,"ColorBytes":{"$binary":{"base64":"QmxhY2s=","subType":"00"}},"ISBN":"111-1111111111","Id2":101.01}')
```

SELECT statement returns the BSON Document object to JDBC API. The client can also retrieve the BSON column value as String, where String is the JSON representation of the BSON document.

