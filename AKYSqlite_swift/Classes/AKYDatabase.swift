//
//  AKYDatabase.swift
//  AKYSqlite
//
//  Created by Aikyuichi on 10/9/19.
//  Copyright (c) 2022 aikyuichi <aikyu.sama@gmail.com>
//

import Foundation
import SQLite3

public class AKYDatabase {
    
    fileprivate let path: String
    fileprivate var sqlite: OpaquePointer? = nil
    fileprivate var transactional = false
    fileprivate var isOpen = false
    fileprivate var rollbackTransaction = false
    fileprivate var deferredAttachments: [String] = []
    
    public var lastInsertRowId: Int {
        Int(sqlite3_last_insert_rowid(self.sqlite))
    }
    
    public var userVersion: Int {
        var version = 0
        let stmt = self.prepareStatement(query: "PRAGMA user_version")
        if stmt.step() {
            version = stmt.getInt(forIndex: 0)!
        }
        stmt.finalize()
        return version
    }
    
    public init(path : String) {
        self.path = path
    }
    
    public init(forKey key: String) {
        let dbPaths = UserDefaults.standard.dictionary(forKey: AKYSqlite.AKYSQLITE_DB_PATHS)
        if let path = dbPaths?[key] as? String {
            self.path = path
        } else {
            self.path = ""
        }
    }
    
    public func open(readonly: Bool = false) -> Bool {
        if readonly {
            if sqlite3_open_v2(self.path, &self.sqlite, SQLITE_OPEN_READONLY, nil) != SQLITE_OK {
                print("error: \(String(cString: sqlite3_errmsg(self.sqlite)))")
                return false
            }
        } else {
            if sqlite3_open(self.path, &self.sqlite) != SQLITE_OK {
                print("error: \(String(cString: sqlite3_errmsg(self.sqlite)))")
                return false
            }
        }
        self.isOpen = true
        for attachement in self.deferredAttachments {
            self.executeQuery(attachement)
        }
        self.deferredAttachments.removeAll()
        return true
    }
    
    public func openTransaction() -> Bool {
        if self.open() {
            self.executeQuery("BEGIN TRANSACTION")
            self.transactional = true
            return true
        }
        return false
    }
    
    public func closeTransaction() {
        if self.transactional {
            if self.rollbackTransaction {
                self.executeQuery("ROLLBACK")
                print("Rollback transaction")
            } else {
                self.executeQuery("COMMIT")
            }
        }
        self.transactional = false
        self.rollbackTransaction = false
    }
    
    public func close() {
        self.closeTransaction()
        sqlite3_close(self.sqlite)
    }
    
    public func attachDatabase(_ database: AKYDatabase, withSchema schema: String) {
        let query = "ATTACH DATABASE '\(database.path)' AS \(schema)"
        if self.isOpen {
            self.executeQuery(query)
        } else {
            self.deferredAttachments.append(query)
        }
        self.executeQuery(query)
    }
    
    public func detachDatabase(withSchema schema: String) {
        let query = "DETACH DATABASE \(schema)"
        self.executeQuery(query)
    }
    
    public func prepareStatement(query: String) -> AKYStatement {
        let statement = AKYStatement(sqlite: self.sqlite, query: query)
        if self.transactional {
            statement.transactionDelegate = self
        }
        return statement
    }
    
    public func executeQuery(_ query: String) {
        var error: UnsafeMutablePointer<CChar>?
        if sqlite3_exec(self.sqlite, query, nil, nil, &error) != SQLITE_OK {
            self.rollbackTransaction = true
            print("execute query failed: \(String(cString: error!))")
            sqlite3_free(error)
        }
    }
    
    public func executeStatement(query: String, parameters: [Any?]) {
        let stmt = self.prepareStatement(query: query)
        for i in 1...parameters.count {
            let parameter = parameters[i - 1]
            stmt.bindValue(parameter, forIndex: Int32(i))
        }
        _ = stmt.step()
        stmt.finalize()
    }
    
    public func executeStatement(query: String, parameters: [String: Any?]) {
        let stmt = self.prepareStatement(query: query)
        for parameter in parameters {
            stmt.bindValue(parameter.value, forName: parameter.key)
        }
        _ = stmt.step()
        stmt.finalize()
    }
    
    public func select(_ query: String, parameters: [Any?]) -> [[String: Any]] {
        var result: [[String: Any]] = []
        let stmt = self.prepareStatement(query: query)
        for i in 1...parameters.count {
            let parameter = parameters[i - 1]
            stmt.bindValue(parameter, forIndex: Int32(i))
        }
        while stmt.step() {
            var row: [String: Any] = [:]
            for i in 0..<stmt.columnCount {
                let columnName = stmt.getColumnName(forIndex: i)
                row[columnName] = stmt.getValue(forIndex: i)
            }
            result.append(row)
        }
        stmt.finalize()
        return result
    }
}

extension AKYDatabase: AKYTransaction {
    func rollback() {
        self.rollbackTransaction = true
    }
}
