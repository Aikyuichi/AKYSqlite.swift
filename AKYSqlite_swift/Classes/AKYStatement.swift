//
//  AKYStatement.swift
//  AKYSqlite
//
//  Created by Aikyuichi on 10/9/19.
//  Copyright (c) 2022 aikyuichi <aikyu.sama@gmail.com>
//

import Foundation
import SQLite3

protocol AKYTransaction {
    func rollback()
}

public class AKYStatement {
    
    private let SQLITE_TRANSIENT = unsafeBitCast(-1, to: sqlite3_destructor_type.self)
    private var sqlite: OpaquePointer? = nil
    private var sqliteStatement: OpaquePointer? = nil
    private var resultColumns: [String: Int32] = [:]
    var transactionDelegate: AKYTransaction?
    public var uncompiledSql = ""
    public var failed = false
    
    @available(iOS 10.0, *)
    public var expandedQuery: String {
        String(cString: sqlite3_expanded_sql(self.sqliteStatement))
    }
    
    public var columnCount: Int32 {
        return sqlite3_column_count(self.sqliteStatement)
    }
    
    init(sqlite: OpaquePointer?, query: String) {
        var uncompiledSql: UnsafePointer<CChar>? = nil
        if sqlite3_prepare_v2(sqlite, query, -1, &self.sqliteStatement, &uncompiledSql) == SQLITE_OK {
            self.sqlite = sqlite
            if let uncompiledSql = uncompiledSql, strlen(uncompiledSql) > 0 {
                self.uncompiledSql = String(cString: uncompiledSql)
                print("warning: uncompiled sql - \(self.uncompiledSql)")
            }
        } else {
            print("prepare statement failed: \(String(cString: sqlite3_errmsg(sqlite)))")
        }
    }
    
    public func step() -> Bool {
        var result = false
        let stepResult = sqlite3_step(self.sqliteStatement)
        if stepResult == SQLITE_ROW {
            result = true
            if self.resultColumns.isEmpty {
                for i in 0..<self.columnCount {
                    self.resultColumns[self.getColumnName(forIndex: i)] = i
                }
            }
        } else if stepResult != SQLITE_DONE {
            self.failed = true
            self.rollback()
            print("step error: \(String(cString: sqlite3_errmsg(self.sqlite)))")
        }
        return result
    }
    
    public func reset() {
        sqlite3_reset(self.sqliteStatement)
    }
    
    public func finalize() {
        sqlite3_finalize(self.sqliteStatement)
    }
    
    public func bindInt(_ value: Int?, forIndex index: Int32) {
        if let value = value {
            sqlite3_bind_int64(self.sqliteStatement, index, Int64(value))
        } else {
            self.bindNULL(forIndex: index)
        }
    }
    
    public func bindString(_ value: String?, forIndex index: Int32) {
        if let stringValue = value {
            sqlite3_bind_text(self.sqliteStatement, index, (stringValue as NSString).utf8String, -1, nil)
        } else {
            self.bindNULL(forIndex: index)
        }
    }
    
    public func bindDouble(_ value: Double?, forIndex index: Int32) {
        if let value = value {
            sqlite3_bind_double(self.sqliteStatement, index, value)
        } else {
            self.bindNULL(forIndex: index)
        }
    }
    
    public func bindBool(_ value: Bool?, forIndex index: Int32) {
        if let value = value {
            sqlite3_bind_int(self.sqliteStatement, index, value ? 1 : 0)
        } else {
            self.bindNULL(forIndex: index)
        }
    }
    
    public func bindData(_ value: Data?, forIndex index: Int32) {
        if let value = value {
            let data = value as NSData
            sqlite3_bind_blob(self.sqliteStatement, index, data.bytes, Int32(data.length), SQLITE_TRANSIENT)
        } else {
            self.bindNULL(forIndex: index)
        }
    }
    
    public func bindValue(_ value: Any?, forIndex index: Int32) {
        switch value {
        case let stringValue as String?:
            self.bindString(stringValue, forIndex: index)
        case let integerValue as Int?:
            self.bindInt(integerValue, forIndex: index)
        case let doubleValue as Double?:
            self.bindDouble(doubleValue, forIndex: index)
        case let boolValue as Bool?:
            self.bindBool(boolValue, forIndex: index)
        case let dataValue as Data?:
            self.bindData(dataValue, forIndex: index)
        default:
            self.bindNULL(forIndex: index)
        }
    }
    
    public func bindInt(_ value: Int?, forName name: String) {
        let index = sqlite3_bind_parameter_index(self.sqliteStatement, name.cString(using: String.Encoding.utf8))
        self.bindInt(value, forIndex: index)
    }
    
    public func bindString(_ value: String?, forName name: String) {
        let index = sqlite3_bind_parameter_index(self.sqliteStatement, name.cString(using: String.Encoding.utf8))
        self.bindString(value, forIndex: index)
    }
    
    public func bindDouble(_ value: Double?, forName name: String) {
        let index = sqlite3_bind_parameter_index(self.sqliteStatement, name.cString(using: String.Encoding.utf8))
        self.bindDouble(value, forIndex: index)
    }
    
    public func bindBool(_ value: Bool?, forName name: String) {
        let index = sqlite3_bind_parameter_index(self.sqliteStatement, name.cString(using: String.Encoding.utf8))
        self.bindBool(value, forIndex: index)
    }
    
    public func bindData(_ value: Data?, forName name: String) {
        let index = sqlite3_bind_parameter_index(self.sqliteStatement, name.cString(using: String.Encoding.utf8))
        self.bindData(value, forIndex: index)
    }
    
    public func bindValue(_ value: Any?, forName name: String) {
        let index = sqlite3_bind_parameter_index(self.sqliteStatement, name.cString(using: String.Encoding.utf8))
        self.bindValue(value, forIndex: index)
    }
    
    public func getColumnName(forIndex index: Int32) -> String {
        return String(cString: sqlite3_column_name(self.sqliteStatement, index))
    }
    
    public func getInt(forIndex index: Int32) -> Int? {
        if self.isColumnNULL(forIndex: index) {
            return nil
        } else {
            return Int(sqlite3_column_int64(self.sqliteStatement, index))
        }
    }
    
    public func getString(forIndex index: Int32) -> String? {
        if self.isColumnNULL(forIndex: index) {
            return nil
        } else {
            return String(cString: sqlite3_column_text(self.sqliteStatement, index))
        }
    }
    
    public func getDouble(forIndex index: Int32) -> Double? {
        if self.isColumnNULL(forIndex: index) {
            return nil
        } else {
            return sqlite3_column_double(self.sqliteStatement, index)
        }
    }
    
    public func getBool(forIndex index: Int32) -> Bool? {
        if self.isColumnNULL(forIndex: index) {
            return nil
        } else {
            return Int(sqlite3_column_int64(self.sqliteStatement, index)) != 0
        }
    }
    
    public func getData(forIndex index: Int32) -> Data? {
        if self.isColumnNULL(forIndex: index) {
            return nil
        } else {
            let length = sqlite3_column_bytes(self.sqliteStatement, index)
            return Data(bytes: sqlite3_column_blob(self.sqliteStatement, index), count: Int(length))
        }
    }
    
    public func getValue(forIndex index: Int32) -> Any? {
        let dataType = sqlite3_column_type(self.sqliteStatement, index)
        switch dataType {
        case SQLITE_INTEGER:
            return self.getInt(forIndex: index)
        case SQLITE_FLOAT:
            return self.getDouble(forIndex: index)
        case SQLITE_TEXT:
            return self.getString(forIndex: index)
        case SQLITE_BLOB:
            return self.getData(forIndex: index)
        case SQLITE_NULL:
            return nil
        default:
            return nil
        }
    }
    
    public func getInt(forName name: String) -> Int? {
        let index = self.resultColumns[name]
        if let index = index {
            return self.getInt(forIndex: index)
        } else {
            return nil
        }
    }
    
    public func getString(forName name: String) -> String? {
        let index = self.resultColumns[name]
        if let index = index {
            return self.getString(forIndex: index)
        } else {
            return nil
        }
    }
    
    public func getDouble(forName name: String) -> Double? {
        let index = self.resultColumns[name]
        if let index = index {
            return self.getDouble(forIndex: index)
        } else {
            return nil
        }
    }
    
    public func getBool(forName name: String) -> Bool? {
        let index = self.resultColumns[name]
        if let index = index {
            return self.getBool(forIndex: index)
        } else {
            return nil
        }
    }
    
    public func getData(forName name: String) -> Data? {
        let index = self.resultColumns[name]
        if let index = index {
            return self.getData(forIndex: index)
        } else {
            return nil
        }
    }
    
    public func getValue(forName name: String) -> Any? {
        if let index = self.resultColumns[name] {
            return self.getValue(forIndex: index)
        }
        return nil
    }
    
    private func isColumnNULL(forIndex index: Int32) -> Bool {
        sqlite3_column_type(self.sqliteStatement, index) == SQLITE_NULL
    }
    
    private func bindNULL(forIndex index: Int32) {
        sqlite3_bind_null(self.sqliteStatement, index)
    }
    
    private func bindFailed() {
        self.rollback()
        print("bind failed: \(String(cString: sqlite3_errmsg(self.sqlite)))")
    }
    
    private func rollback() {
        self.transactionDelegate?.rollback()
    }
}
