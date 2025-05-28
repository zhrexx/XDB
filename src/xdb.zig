const std = @import("std");
const ArrayList = std.ArrayList;
const crypto = std.crypto;
const mem = std.mem;

var allocator = std.heap.page_allocator;

pub fn getAllocator() std.mem.Allocator {
    return allocator;
}

const LogLevel = enum {
    FULL, 
    NOTHING
};

pub var log_level: LogLevel = .FULL;

pub fn log(comptime msg: []const u8, args: anytype) void {
    if (log_level == .FULL) {
        std.debug.print("[XDB] " ++ msg, args);
    }
}

const db_magic: [4]u8 = @constCast("XDB1").*;

pub const User = struct {
    user: []const u8,
    password_hash: [32]u8,
};

pub const Word = union(enum) {
    string: []u8,
    int: i64,
    float: f64,
    boolean: bool,

    pub fn deinit(self: Word) void {
        switch (self) {
            .string => |s| allocator.free(s),
            else => {},
        }
    }
};

pub const Column = struct {
    values: ArrayList(Word),

    pub fn deinit(self: *Column) void {
        for (self.values.items) |value| {
            value.deinit();
        }
        self.values.deinit();
    }
};

pub const Row = struct {
    id: []u8,
    columns: ArrayList(Column),

    pub fn deinit(self: *Row) void {
        for (self.columns.items) |*column| {
            column.deinit();
        }
        self.columns.deinit();
        allocator.free(self.id);
    }
};

pub const Table = struct {
    id: []u8,
    rows: ArrayList(Row),

    pub fn deinit(self: *Table) void {
        for (self.rows.items) |*row| {
            row.deinit();
        }
        self.rows.deinit();
        allocator.free(self.id);
    }
};

pub const DatabaseError = error{
    InvalidDatabaseFormat,
    TableAlreadyExists,
    TableNotFound,
    RowAlreadyExists,
    RowNotFound,
    InvalidValueCount,
    InvalidColumnIndex,
    InvalidTypeTag,
    UserAlreadyExists,
    UserNotFound,
    QueryFailed,
};

pub const Database = struct {
    magic: [4]u8 = db_magic,
    users: ArrayList(User),
    tables: ArrayList(Table),

    pub fn init() Database {
        return .{ .users = ArrayList(User).init(allocator), .tables = ArrayList(Table).init(allocator) };
    }

    pub fn initWithUser(user: []const u8, password: []const u8) !Database {
        var users = ArrayList(User).init(allocator);
        const password_hash = try hashPassword(password);
        try users.append(.{ .user = try allocator.dupe(u8, user), .password_hash = password_hash });
        return .{ .users = users, .tables = ArrayList(Table).init(allocator) };
    }

    pub fn save(self: *Database, filename: []const u8, key: [32]u8) !void {
        var serial_buffer = ArrayList(u8).init(allocator);
        defer serial_buffer.deinit();
        var writer = serial_buffer.writer();
        try writer.writeAll(&self.magic);
        try writer.writeInt(u32, @intCast(self.users.items.len), .little);
        for (self.users.items) |user| {
            try writer.writeInt(u32, @intCast(user.user.len), .little);
            try writer.writeAll(user.user);
            try writer.writeAll(&user.password_hash);
        }
        try writer.writeInt(u32, @intCast(self.tables.items.len), .little);
        for (self.tables.items) |table| {
            try writer.writeInt(u32, @intCast(table.id.len), .little);
            try writer.writeAll(table.id);
            try writer.writeInt(u32, @intCast(table.rows.items.len), .little);
            for (table.rows.items) |row| {
                try writer.writeInt(u32, @intCast(row.id.len), .little);
                try writer.writeAll(row.id);
                try writer.writeInt(u32, @intCast(row.columns.items.len), .little);
                for (row.columns.items) |column| {
                    try writer.writeInt(u32, @intCast(column.values.items.len), .little);
                    for (column.values.items) |value| {
                        switch (value) {
                            .string => |s| {
                                try writer.writeByte(0);
                                try writer.writeInt(u32, @intCast(s.len), .little);
                                try writer.writeAll(s);
                            },
                            .int => |i| {
                                try writer.writeByte(1);
                                try writer.writeInt(i64, i, .little);
                            },
                            .float => |f| {
                                try writer.writeByte(2);
                                try writer.writeInt(u64, @bitCast(f), .little);
                            },
                            .boolean => |b| {
                                try writer.writeByte(3);
                                try writer.writeByte(if (b) 1 else 0);
                            },
                        }
                    }
                }
            }
        }
        const plaintext = serial_buffer.items;
        const Aes256Gcm = std.crypto.aead.aes_gcm.Aes256Gcm;
        var nonce: [Aes256Gcm.nonce_length]u8 = undefined;
        std.crypto.random.bytes(&nonce);
        const ciphertext = try allocator.alloc(u8, plaintext.len);
        defer allocator.free(ciphertext);
        var tag: [Aes256Gcm.tag_length]u8 = undefined;
        Aes256Gcm.encrypt(ciphertext, &tag, plaintext, "", nonce, key);
        const file = try std.fs.cwd().createFile(filename, .{});
        defer file.close();
        try file.writeAll(&nonce);
        try file.writeAll(ciphertext);
        try file.writeAll(&tag);
        log("Saved Database with {d} users and {d} tables\n", .{ self.users.items.len, self.tables.items.len });
    }

    pub fn load(filename: []const u8, key: [32]u8) !Database {
        const Aes256Gcm = std.crypto.aead.aes_gcm.Aes256Gcm;
        const file = try std.fs.cwd().openFile(filename, .{});
        defer file.close();
        const encrypted_data = try file.reader().readAllAlloc(allocator, std.math.maxInt(usize));
        defer allocator.free(encrypted_data);
        if (encrypted_data.len < Aes256Gcm.nonce_length + Aes256Gcm.tag_length) return DatabaseError.InvalidDatabaseFormat;
        const nonce = encrypted_data[0..Aes256Gcm.nonce_length];
        const tag = encrypted_data[encrypted_data.len - Aes256Gcm.tag_length..];
        const ciphertext = encrypted_data[Aes256Gcm.nonce_length..encrypted_data.len - Aes256Gcm.tag_length];
        const plaintext = try allocator.alloc(u8, ciphertext.len);
        defer allocator.free(plaintext);
        var nonce_array: [Aes256Gcm.nonce_length]u8 = undefined;
        @memcpy(&nonce_array, nonce);
        var tag_array: [Aes256Gcm.tag_length]u8 = undefined;
        @memcpy(&tag_array, tag);
        Aes256Gcm.decrypt(plaintext, ciphertext, tag_array, "", nonce_array, key) catch return DatabaseError.InvalidDatabaseFormat;
        var stream = std.io.fixedBufferStream(plaintext);
        const reader = stream.reader();
        var magic: [4]u8 = undefined;
        try reader.readNoEof(&magic);
        if (!mem.eql(u8, &magic, "XDB3")) {
            return DatabaseError.InvalidDatabaseFormat;
        }
        var db = Database{ .users = ArrayList(User).init(allocator), .tables = ArrayList(Table).init(allocator) };
        const user_count = try reader.readInt(u32, .little);
        try db.users.ensureTotalCapacity(user_count);
        var i: u32 = 0;
        while (i < user_count) : (i += 1) {
            const user_len = try reader.readInt(u32, .little);
            const user = try allocator.alloc(u8, user_len);
            try reader.readNoEof(user);
            var password_hash: [32]u8 = undefined;
            try reader.readNoEof(&password_hash);
            try db.users.append(.{ .user = user, .password_hash = password_hash });
        }
        const table_count = try reader.readInt(u32, .little);
        try db.tables.ensureTotalCapacity(table_count);
        i = 0;
        while (i < table_count) : (i += 1) {
            const table_id_len = try reader.readInt(u32, .little);
            const table_id = try allocator.alloc(u8, table_id_len);
            try reader.readNoEof(table_id);
            var table = Table{ .rows = ArrayList(Row).init(allocator), .id = table_id };
            const row_count = try reader.readInt(u32, .little);
            try table.rows.ensureTotalCapacity(row_count);
            var j: u32 = 0;
            while (j < row_count) : (j += 1) {
                const id_len = try reader.readInt(u32, .little);
                const id = try allocator.alloc(u8, id_len);
                try reader.readNoEof(id);
                var row = Row{ .id = id, .columns = ArrayList(Column).init(allocator) };
                const column_count = try reader.readInt(u32, .little);
                try row.columns.ensureTotalCapacity(column_count);
                var k: u32 = 0;
                while (k < column_count) : (k += 1) {
                    var column = Column{ .values = ArrayList(Word).init(allocator) };
                    const value_count = try reader.readInt(u32, .little);
                    try column.values.ensureTotalCapacity(value_count);
                    var m: u32 = 0;
                    while (m < value_count) : (m += 1) {
                        const tag_ = try reader.readByte();
                        switch (tag_) {
                            0 => {
                                const len = try reader.readInt(u32, .little);
                                const str = try allocator.alloc(u8, len);
                                try reader.readNoEof(str);
                                try column.values.append(.{ .string = str });
                            },
                            1 => {
                                const i_val = try reader.readInt(i64, .little);
                                try column.values.append(.{ .int = i_val });
                            },
                            2 => {
                                const u = try reader.readInt(u64, .little);
                                const f: f64 = @bitCast(u);
                                try column.values.append(.{ .float = f });
                            },
                            3 => {
                                const b_byte = try reader.readByte();
                                const b = b_byte != 0;
                                try column.values.append(.{ .boolean = b });
                            },
                            else => return DatabaseError.InvalidTypeTag,
                        }
                    }
                    try row.columns.append(column);
                }
                try table.rows.append(row);
            }
            try db.tables.append(table);
        }
        log("Loaded Database with {d} users and {d} tables\n", .{ db.users.items.len, db.tables.items.len });
        return db;
    }


    pub fn saveWithKey(self: *Database, filename: []const u8, key: []const u8) !void {
        var key_array: [32]u8 = [_]u8{'0'} ** 32;
        const key_len = @min(key.len, 32);
        @memcpy(key_array[0..key_len], key[0..key_len]);
        try self.save(filename, key_array);
    }

    pub fn loadWithKey(filename: []const u8, key: []const u8) !Database {
        var key_array: [32]u8 = [_]u8{'0'} ** 32;
        const key_len = @min(key.len, 32);
        @memcpy(key_array[0..key_len], key[0..key_len]);
        return try Database.load(filename, key_array);
    }

    pub fn createTable(self: *Database, table_id: []const u8) !void {
        for (self.tables.items) |table| {
            if (mem.eql(u8, table.id, table_id)) {
                return DatabaseError.TableAlreadyExists;
            }
        }
        const table = Table{ .id = try allocator.dupe(u8, table_id), .rows = ArrayList(Row).init(allocator) };
        try self.tables.append(table);
    }

    pub fn addRow(self: *Database, table_id: []const u8, row_id: []const u8) !void {
        for (self.tables.items) |*table| {
            if (mem.eql(u8, table.id, table_id)) {
                for (table.rows.items) |row| {
                    if (mem.eql(u8, row.id, row_id)) {
                        return DatabaseError.RowAlreadyExists;
                    }
                }
                const row = Row{ .id = try allocator.dupe(u8, row_id), .columns = ArrayList(Column).init(allocator) };
                try table.rows.append(row);
                return;
            }
        }
        return DatabaseError.TableNotFound;
    }

    pub fn addColumn(self: *Database, table_id: []const u8, row_id: []const u8, values: []const Word) !void {
        for (self.tables.items) |*table| {
            if (mem.eql(u8, table.id, table_id)) {
                for (table.rows.items) |*row| {
                    if (mem.eql(u8, row.id, row_id)) {
                        var column = Column{ .values = ArrayList(Word).init(allocator) };
                        for (values) |value| {
                            const val = switch (value) {
                                .string => |s| Word{ .string = try allocator.dupe(u8, s) },
                                else => value,
                            };
                            try column.values.append(val);
                        }
                        try row.columns.append(column);
                        return;
                    }
                }
                return DatabaseError.RowNotFound;
            }
        }
        return DatabaseError.TableNotFound;
    }

    pub fn getRow(self: *Database, table_id: []const u8, row_id: []const u8) ?Row {
        for (self.tables.items) |table| {
            if (mem.eql(u8, table.id, table_id)) {
                for (table.rows.items) |row| {
                    if (mem.eql(u8, row.id, row_id)) return row;
                }
            }
        }
        return null;
    }

    pub fn getColumns(self: *Database, table_id: []const u8) ?[]Column {
        for (self.tables.items) |table| {
            if (mem.eql(u8, table.id, table_id)) {
                return if (table.rows.items.len > 0) table.rows.items[0].columns.items else &[_]Column{};
            }
        }
        return null;
    }

    pub fn queryColumns(self: *Database, table_id: []const u8, row_id: []const u8, predicate: fn (Word) bool) !ArrayList(Column) {
        var result = ArrayList(Column).init(allocator);
        for (self.tables.items) |table| {
            if (mem.eql(u8, table.id, table_id)) {
                for (table.rows.items) |row| {
                    if (mem.eql(u8, row.id, row_id)) {
                        for (row.columns.items, 0..) |column, i| {
                            if (column.values.items.len > 0 and predicate(column.values.items[0])) {
                                var new_column = Column{ .values = ArrayList(Word).init(allocator) };
                                for (row.columns.items[i].values.items) |value| {
                                    const val = switch (value) {
                                        .string => |s| Word{ .string = try allocator.dupe(u8, s) },
                                        else => value,
                                    };
                                    try new_column.values.append(val);
                                }
                                try result.append(new_column);
                            }
                        }
                        return result;
                    }
                }
                return DatabaseError.RowNotFound;
            }
        }
        return DatabaseError.TableNotFound;
    }

    pub fn updateColumn(self: *Database, table_id: []const u8, row_id: []const u8, column_index: usize, values: []const Word) !void {
        for (self.tables.items) |*table| {
            if (mem.eql(u8, table.id, table_id)) {
                for (table.rows.items) |*row| {
                    if (mem.eql(u8, row.id, row_id)) {
                        if (column_index >= row.columns.items.len) {
                            return DatabaseError.InvalidColumnIndex;
                        }
                        if (values.len != row.columns.items[column_index].values.items.len) {
                            return DatabaseError.InvalidValueCount;
                        }
                        for (row.columns.items[column_index].values.items) |*value| {
                            value.deinit();
                        }
                        row.columns.items[column_index].values.clearRetainingCapacity();
                        for (values) |value| {
                            const new_value = switch (value) {
                                .string => |s| Word{ .string = try allocator.dupe(u8, s) },
                                else => value,
                            };
                            try row.columns.items[column_index].values.append(new_value);
                        }
                        return;
                    }
                }
                return DatabaseError.RowNotFound;
            }
        }
        return DatabaseError.TableNotFound;
    }

    pub fn deleteColumn(self: *Database, table_id: []const u8, row_id: []const u8, column_index: usize) !void {
        for (self.tables.items) |*table| {
            if (mem.eql(u8, table.id, table_id)) {
                for (table.rows.items) |*row| {
                    if (mem.eql(u8, row.id, row_id)) {
                        if (column_index >= row.columns.items.len) {
                            return DatabaseError.InvalidColumnIndex;
                        }
                        row.columns.items[column_index].deinit();
                        _ = row.columns.orderedRemove(column_index);
                        return;
                    }
                }
                return DatabaseError.RowNotFound;
            }
        }
        return DatabaseError.TableNotFound;
    }

    pub fn deleteRow(self: *Database, table_id: []const u8, row_id: []const u8) !void {
        for (self.tables.items) |*table| {
            if (mem.eql(u8, table.id, table_id)) {
                for (table.rows.items, 0..) |*row, i| {
                    if (mem.eql(u8, row.id, row_id)) {
                        row.deinit();
                        _ = table.rows.orderedRemove(i);
                        return;
                    }
                }
                return DatabaseError.RowNotFound;
            }
        }
        return DatabaseError.TableNotFound;
    }

    pub fn deleteTable(self: *Database, table_id: []const u8) !void {
        for (self.tables.items, 0..) |*table, i| {
            if (mem.eql(u8, table.id, table_id)) {
                table.deinit();
                _ = self.tables.orderedRemove(i);
                return;
            }
        }
        return DatabaseError.TableNotFound;
    }

    pub fn listTables(self: *Database) []const []const u8 {
        var result = allocator.alloc([]u8, self.tables.items.len) catch return &[_][]u8{};
        for (self.tables.items, 0..) |table, i| {
            result[i] = table.id;
        }
        return result;
    }

    pub fn listRows(self: *Database, table_id: []const u8) ![]const []const u8 {
        for (self.tables.items) |table| {
            if (mem.eql(u8, table.id, table_id)) {
                var result = try allocator.alloc([]u8, table.rows.items.len);
                for (table.rows.items, 0..) |row, i| {
                    result[i] = row.id;
                }
                return result;
            }
        }
        return DatabaseError.TableNotFound;
    }

    pub fn listUsers(self: *Database) []const []const u8 {
        var result = allocator.alloc([]u8, self.users.items.len) catch return &[_][]u8{};
        for (self.users.items, 0..) |user, i| {
            result[i] = @constCast(user.user);
        }
        return result;
    }

    pub fn addUser(self: *Database, user: []const u8, password: []const u8) !void {
        for (self.users.items) |u| {
            if (mem.eql(u8, u.user, user)) {
                return DatabaseError.UserAlreadyExists;
            }
        }
        const password_hash = try hashPassword(password);
        try self.users.append(.{ .user = try allocator.dupe(u8, user), .password_hash = password_hash });
    }

    pub fn verifyUser(self: *Database, user: []const u8, password: []const u8) !bool {
        for (self.users.items) |u| {
            if (mem.eql(u8, u.user, user)) {
                const hash = try hashPassword(password);
                return mem.eql(u8, &u.password_hash, &hash);
            }
        }
        return DatabaseError.UserNotFound;
    }

    pub fn removeUser(self: *Database, user: []const u8) !void {
        for (self.users.items, 0..) |u, i| {
            if (mem.eql(u8, u.user, user)) {
                allocator.free(u.user);
                _ = self.users.orderedRemove(i);
                return;
            }
        }
        return DatabaseError.UserNotFound;
    }

    pub fn updateUserPassword(self: *Database, user: []const u8, new_password: []const u8) !void {
        for (self.users.items) |*u| {
            if (mem.eql(u8, u.user, user)) {
                u.password_hash = try hashPassword(new_password);
                return;
            }
        }
        return DatabaseError.UserNotFound;
    }

    pub fn deinit(self: *Database) void {
        for (self.users.items) |user| {
            allocator.free(user.user);
        }
        self.users.deinit();
        for (self.tables.items) |*table| {
            table.deinit();
        }
        self.tables.deinit();
    }
};

fn hashPassword(password: []const u8) ![32]u8 {
    var hash: [32]u8 = undefined;
    crypto.hash.sha2.Sha256.hash(password, &hash, .{});
    return hash;
}
