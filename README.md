# AKYSqlite_swift

<!-- [![CI Status](https://img.shields.io/travis/aikyuichi/AKYSqlite_swift.svg?style=flat)](https://travis-ci.org/aikyuichi/AKYSqlite_swift) -->
[![Version](https://img.shields.io/cocoapods/v/AKYSqlite_swift.svg?style=flat)](https://cocoapods.org/pods/AKYSqlite_swift)
[![License](https://img.shields.io/cocoapods/l/AKYSqlite_swift.svg?style=flat)](https://cocoapods.org/pods/AKYSqlite_swift)
[![Platform](https://img.shields.io/cocoapods/p/AKYSqlite_swift.svg?style=flat)](https://cocoapods.org/pods/AKYSqlite_swift)

## Installation

AKYSqlite.swift is available through [CocoaPods](https://cocoapods.org). To install
it, simply add the following line to your Podfile:

```ruby
pod 'AKYSqlite_swift'
```
Import AKYSqlite.h
```Swift
import AKYSqlite_swift
```

### Don't want to use CocoaPods?

Copy files from AKYSqlite_swift/Classes to your project

## Usage
```Swift
let dbPath = "/path/to/the/database/file"
let db = AKYDatabase(path: dbPath)
if db.open() {
    let stmt = db.prepareStatement(query: "SELECT name, lastname FROM person WHERE person_id = @id")
    stmt.bindInt(1, forName: "@id")
    while stmt.step() {
        let name = stmt.getString(forName: "name")!
        let lastname = stmt.getString(forName: "lastname")
    }
    stmt.finalize()
    db.close()
}

```

## Author

Aikyuichi, aikyu.sama@gmail.com

## License

AKYSqlite.swift is available under the MIT license. See the LICENSE file for more info.
