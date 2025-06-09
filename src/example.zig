const XDB = @import("XDB");
const std = @import("std");

pub fn printRow(row: XDB.Row) void {
    std.debug.print("Row '{s}':\n", .{row.id});
    for (row.columns.items, 0..) |column, col_index| {
        std.debug.print(" - Column {d}: ", .{col_index});
        for (column.values.items, 0..) |value, value_index| {
            switch (value) {
                .string => |s| std.debug.print("{s}", .{s}),
                .int => |i| std.debug.print("{d}", .{i}),
                .float => |f| std.debug.print("{}", .{f}),
                .boolean => |b| std.debug.print("{s}", .{if (b) "true" else "false"}),
            }
            if (value_index + 1 < column.values.items.len)
                std.debug.print(", ", .{});
        }
        std.debug.print("\n", .{});
    }
}

pub fn main() !u8 {
    var db = try XDB.Database.initWithUser("HelloWorld", "aboba_pass#123");
    defer db.deinit();
    try db.createTable("hello_abobus");
    try db.addRow("hello_abobus", "row1");
    try db.addColumn("hello_abobus", "row1", &.{ .{.int =  10} });
    try db.addColumn("hello_abobus", "row1", &.{.{ .string = @constCast("Hello World") }});
    try db.saveWithKey("a.xdb", "aboba");

    db = undefined;

    db = try XDB.Database.loadWithKey("a.xdb", "aboba");
    const row = db.getRow("hello_abobus", "row1");
    printRow(row.?);
    return 0;
}


