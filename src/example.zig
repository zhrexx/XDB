const XDB = @import("XDB");
const std = @import("std");

pub fn main() !u8 {
    var db = try XDB.Database.initWithUser("Hello, World", "aboba_pass#123");
    defer db.deinit();
    try db.createTable("hello_abobus");
    try db.addRow("hello_abobus", "row1");
    try db.addColumn("hello_abobus", "row1", &.{ .{.int =  10}});
    try db.save("aboba.xdb");
    return 0;
}
