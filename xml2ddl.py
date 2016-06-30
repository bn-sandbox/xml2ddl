#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# XML to DDL converter
# 2012 Fridolin Pokorny <fridex.devel@gmail.com>

import getopt
import sys
import io
import re
import xml.etree.ElementTree as etree
import xml.parsers.expat as parsers

# Exception used if arguments are not correct.
class XTDCheckArgument(Exception):
    pass

# Exception used if there was an error with input file.
class XTDIError(Exception):
    pass

# Exception used if there was an error with output file.
class XTDOError(Exception):
    pass

# Exception used if database generated from isvalid is not valid.
class XTDNotValid(Exception):
    pass

# Exception used if attribute attribute or table has name, which can confuse
# relations.
class XTDNameError(Exception):
    pass

################################################################################
# Database class basic operations.
class Database:
    """Database class basic operations on XTD."""
    # @param - etc option from command line
    # @param - enable duplicit tables
    # @param - do not generate columns from attributes
    # @return - none
    def __init__(self, etc = -1, duplicity = 0, no_columns = 0):
        self.__etc          = etc
        self.__duplicity    = duplicity
        self.__entries      = {}
        self.__no_columns   = no_columns
        self.__relations    = {}

    # Update relations in table by name.
    # @param - name of the table to update relations
    # @param - dict with table name and relation count
    # @return - none
    def update_relations(self, name, relations):
        if name not in self.__entries:
            self.__entries[name] = Table(name)

        self.__entries[name].update_relations(relations)

    # Update type in value in value column.
    # @param - name of table in database
    # @param - data in value column to determinate data type
    # @return - none
    def update_value(self, name, data):
        if name not in self.__entries:
            self.__entries[name] = Table(name)

        self.__entries[name].update_value(data)

    # Get columns of given table.
    # @param - table name to get columns from
    # @return - dict with columns and their types
    def columns(self, table):
        if table in self.__entries:
            return self.__entries[table].columns()
        else:
            return None

    # Get all tables in database.
    # @return - all tables in db
    def entries(self):
        return self.__entries

    # Get foreign keys of given table.
    # @param - table name to get keys from
    # @return - dict with keys
    def keys(self, table):
        if table in self.__entries:
            return self.__entries[table].keys()
        else:
            return None

    # Get type of value field.
    # @param - table name to get value from
    # @return - value data type
    def value(self, table):
        if table in self.__entries:
            return self.__entries[table].value()
        else:
            return None

    # Update column for attributes in table by name. If table does not exist it
    # is created
    # @param - name if table in database
    # @param - attribute name
    # @param - data in attribute to determinate data type
    # @return - none
    def update_attribute(self, name, attribute, data):
        if not self.__no_columns:
            if name not in self.__entries:
                self.__entries[name] = Table(name)

            self.__entries[name].update_attribute(attribute, data)

    # Update database structure before print. Tables from relations are created.
    # @param - none
    # @return - none
    def flush(self):
        self.__relations = {}

        for table in self.__entries.values():
            self.__relations[table.name()] = set([])

        for table in self.__entries.values():
            for name, count in table.relations().items():
                if self.__duplicity:
                    if name in table.columns().keys():
                        raise XTDNameError

                    # Get only one i_item with highest ranking of datatype.
                    table.set_key(name)
                    # Store info for XML generation.
                    self.__relations[table.name()].add(name)

                elif self.__etc != -1 and count > self.__etc:
                    if name in self.__entries[name].columns().keys():
                        raise XTDNameError

                    # Maximum in-table reference reached.
                    # Create records in table.
                    if name not in self.__entries:
                        self.__entries[name] = Table(name)

                    self.__entries[name].set_key(table.name())
                    self.__relations[name].add(table.name())

                else:
                    # Maximum in-table reference not reached.
                    # create records in subtable
                    if count == 1:
                        if name in table.columns().keys():
                            raise XTDNameError

                        table.set_key(name)
                    else:
                        for num in range(count):
                            if (name + str(num  + 1)) in table.columns().keys():
                                raise XTDNameError

                            table.set_key(name + str(num + 1))
                    self.__relations[table.name()].add(name)

    # Print database structure in DDL format.
    # @param - output file to print to
    # @return - none
    def print_ddl(self, fout):
        self.flush()

        for table in self.__entries.values():
            fout.write("CREATE TABLE " + table.name() + "("
                       + "\n   prk_" + table.name() + "_id" + " INT PRIMARY KEY")
            # Print foreign keys.
            for key in table.keys():
                fout.write(",\n   " + key + " INT")
            # Print columns and their data types.
            for column, data_type in table.columns().items():
                fout.write(",\n   " + column + " " + data_type)
            # Print value, if any.
            data_type = table.value()
            if data_type != None:
                fout.write(",\n   value " + data_type)

            fout.write("\n);\n\n")

    # Print given relation.
    # @param - output file to print to
    # @param - relation type to print
    # @param - relation to table
    # @return - none
    def print_relation(self, fout, relation_type, table):
        fout.write("        ")
        fout.write("<relation to=\""
                       + table + "\" relation_type=\""
                       + relation_type + "\" \>\n")

    # Browse through tables and print their relations.
    # @param - output file to write to
    # @param - starting table
    # @param - table to start from
    # @param - table from the relation is inspected
    # @param - relation type to inspect
    # @param - 1 if 1:1 relation was printed, otherwise 0
    # @return -  none
    def print_tablerel(self, fout, toriginal, tname_from, tname_to,
                       rel, printed, route):
        #print(route, end=" ")
        #print("from: " + tname_from + " to " + tname_to + " " + str(rel))

        # This can be a magic section for many people... GOOD LOCK!

        # There can be 3 possible relations between tables - N:1 (0), 1:N (1)
        # and N:M (2). 1:1 is a special relation which ends recursion. It ends
        # when no other relations from table are possible, as well. Every time
        # it has to be checked whether we are not comming to previous table, it
        # can cause stack overflow (infinite loop)... and it will! Information
        # whether 1:1 relation was printed is propagated through last argument,
        # when returning from recursion through return value (1:1 is printed
        # obly once per table). That's it... ;-)
        if rel == 0:
            if tname_to in self.__relations[tname_from]\
                    and tname_to not in route:

                # If there is cycle between tables e.g. A 1:N B and A: N:1 B,
                # print only once N:M and inspect only one direction.
                if tname_from in self.__relations[tname_to]:
                    self.print_relation(fout, "N:M", tname_to)
                else:
                    self.print_relation(fout, "N:1", tname_to)

                route.append(tname_to)

                # Check my foreign keys.
                for table in self.__relations[tname_to]:
                    if table == toriginal and not printed:
                        self.print_relation(fout, "1:1", table)
                        route.append(table)
                        printed = 1
                    elif table not in route:
                        printed = self.print_tablerel(fout, toriginal, tname_to,
                                                    table, 0, printed, route)

                # Check for tables, which are referencing me.
                for table in self.__entries.keys():
                    if tname_to in self.__relations[table]:
                        if table == toriginal and not printed:
                            self.print_relation(fout, "1:1", table)
                            route.append(table)
                            printed = 1
                        if table != toriginal and table not in route:
                            # From relation N:1, there will be N:M relation (2).
                            printed = self.print_tablerel(fout, toriginal,
                                                       tname_to, table, 2,
                                                       printed, route)
        elif rel == 1:
            # If there is cycle between tables e.g. A 1:N B and A: N:1 B,
            # print only once N:M and inspect only one direction - rel == 0.
            if tname_from in self.__relations[tname_to] \
                    and tname_to not in route:

                if tname_to in self.__relations[tname_from]:
                    if tname_from != toriginal:
                        self.print_relation(fout, "N:M", tname_to)
                else:
                    self.print_relation(fout, "1:N", tname_to)

                route.append(tname_to)


                # Check my foreign keys.
                for table in self.__relations[tname_to]:
                    if table == toriginal and not printed:
                        self.print_relation(fout, "1:1", table)
                        route.append(table)
                        printed = 1
                    elif table not in route:
                        printed = self.print_tablerel(fout, toriginal, tname_to,
                                                    table, 2, printed, route)

                # Check for tables, which are referencing me.
                for table in self.__entries.keys():
                    if tname_to in self.__relations[table]:
                        if table == toriginal and not printed:
                            self.print_relation(fout, "1:1", table)
                            route.append(table)
                            printed = 1
                        if table != toriginal and table not in route:
                            # From relation 1:N, there will be 1:M relation (1).
                            printed = self.print_tablerel(fout, toriginal,
                                                       tname_to, table, 1,
                                                       printed, route)
        elif rel == 2:
            if tname_to not in route and tname_to != toriginal:
                self.print_relation(fout, "N:M", tname_to)

                route.append(tname_to)

                # Check my foreign keys.
                for table in self.__relations[tname_to]:
                    if table == toriginal and not printed:
                        self.print_relation(fout, "1:1", table)
                        route.append(table)
                        printed = 1
                    elif table not in route:
                        # From relation N:N, there will be N:M relation (2).
                        printed = self.print_tablerel(fout, toriginal, tname_to,
                                                   table, 2, printed, route)

                # Check for tables, which are referencing me.
                for table in self.__entries.keys():
                    if tname_to in self.__relations[table]:
                        if table == toriginal and not printed:
                            self.print_relation(fout, "1:1", table)
                            route.append(table)
                            printed = 1
                        if table != toriginal and table not in route:
                            # From relation N:N, there will be N:M relation (2).
                            printed = self.print_tablerel(fout, toriginal, tname_to,
                                                       table, 2, printed, route)

        # Propagate information if 1:1 relation was written.
        return printed

                   #########################################
                   #                                       #
                   #            #####                      #
                   #           #### _\_  ________          #
                   #           ##=-[.].]| \      \         #
                   #           #(  ' _\ |  |------|        #
                   #            #   __| |  ||||||||        #
                   #             \  _/  |  ||||||||        #
                   #          .--'--'-. |  | ____ |        #
                   #         / __      `|__|[o__o]|        #
                   #       _(____nm_______ /____\____      #
                   #                                       #
           ##########################################################
           # It was easier to write it then understand it... I bet! #
           ##########################################################

    # Print structure of the database in xml format.
    # @param - output file to print to
    # @return - none
    def print_xmlrel(self, fout):
        self.flush()
        fout.write("<tables>\n")
        for table1 in self.__entries.keys():
            fout.write("    <table name=\"" + table1 + "\">\n")
            printed = 0
            route = []
            for table2 in self.__entries.keys():
                if table2 != table1:
                    printed = self.print_tablerel(fout, table1, table1, table2,
                                                  0, printed, route)

            for table2 in self.__entries.keys():
                if table1 in self.__relations[table2]:
                    if table1 == table2 and not printed:
                        self.print_relation(fout, "1:1", table1)
                        route.append(table1)
                        printed = 1
                    else:
                        printed = self.print_tablerel(fout, table1, table1, table2,
                                                      1, printed, route)

            fout.write("    </table>\n")
        fout.write("</tables>\n")

    # Validate given table e. g. check if it is subset.
    # @param - table to be checked
    # @return - 1 if table is valid, otherwise 0
    def is_subset(self, db):
        for tname, table in db.entries().items():
            if tname not in self.__entries:
                return 0

            # Check columns.
            for column, data_type in table.columns().items():
                if column not in self.__entries[tname].columns():
                    return 0
                if not data_type_usable(self.__entries[tname].columns()[column],
                                        data_type):
                    return 0

            # Check value field.
            if not data_type_usable(self.__entries[tname].value(),
                                    table.value()):
                return 0

        return 1

################################################################################
# Table class to represent table record in database.
class Table:
    """Table class to represent table record in database for XTD."""
    # Constructor.
    # @param - name of the table
    def __init__(self, name):
        self.__name      = name
        self.__columns   = {}
        self.__relations = {}
        self.__keys      = {}
        self.__refs      = {}
        self.__value     = None

    # Getter for name.
    # @param - none
    # @return - table name
    def name(self):
        return self.__name

    # Getter for columns.
    # @param - none
    # @return - dict with column name as a key and data type as a value
    def columns(self):
        return self.__columns

    # Getter for relations.
    # @param - none
    # @return - dict with name of the table as a key and reference count as a
    # value
    def relations(self):
        return self.__relations

    # Delete relation record for given table.
    # @param - table name to be deleted
    def del_relation(self, name):
        if name in self.__relations:
            del self.__relations[name]

    # Returns list of foreign keys in table.
    # @param - none
    # @return - list of foreign keys name
    def keys(self):
        return self.__keys.keys()

    # Return type of value field.
    # @param - none
    # @return - None if value is not set, otherwise data type of value
    def value(self):
        return self.__value

    # Update relations in table.
    # @param - dict with referenced table as a name and reference count as a key
    # @return - none
    def update_relations(self, relations):
        for rel, count in relations.items():
            self.__relations[rel] = max(self.__relations.get(rel, 0), count)

    def set_relation(self, tname, count):
        self.__relations[tname] = count

    # Update column in table and get data type for record. If column does not
    # exist, it is created.
    # @param - column name to update
    # @oaram - column data to determinate data type
    # @return - none
    def update_attribute(self, column, data):
        if column == "value":
            self.update_value(data)
        else:
            if column == "prk_" + self.name() + "_id":
                raise XTDNameError # Cannot add atribute with same name as PRK!

            self.__columns[column] = get_data_type(data,
                                               self.__columns.get(column, "BIT"))

    def set_key(self, ref):
        fkname = ref + "_id"
        if fkname in self.__columns:
            raise XTDNameError # There is column with same name!

        self.__keys[fkname] = "INT"

    # Update value column in the table and get data type for record. If column
    # does not exist, it is created.
    # @oaram - value data to determinate data type
    # @return - none
    def update_value(self, data):
        self.__value = get_data_type(data,
                                     self.__columns.get("value", "BIT"), 1)

################################################################################
# Determinate data type by data value and previous data type.
# @param - data which column holds
# @param - previous data type
# @param - 1 if generating value, otherwise 0
def get_data_type(data, data_type = "BIT", value = 0):
    if data == "":
        indata_type = "BIT"
    elif re.search("(^1$)|(^0$)|(^True$)|(^False$)", data):
        indata_type = "BIT"
    elif re.search("^[0-9]+$", data):
        indata_type = "INT"
    elif re.search("^[-+]?\d*\.?\d+((e|E)[-+]?\d+)?$", data):
        indata_type = "FLOAT"
    elif not value:
        indata_type = "NVARCHAR"
    else:
        indata_type = "NTEXT"

    if data_type == "BIT":
        return indata_type
    elif data_type == "INT" and indata_type == "BIT" or indata_type == "INT":
        return data_type
    elif data_type == "FLOAT" \
       and indata_type == "BIT" or indata_type == "INT" or indata_type == "FLOAT":
        return data_type
    elif data_type == "NVARCHAR" and indata_type != "NTEXT":
        return data_type
    elif data_type == "NTEXT":
        return data_type
    else:
        return indata_type

################################################################################
# Check if data2 can be stored in data1.
# @param - data type to assign to
# @param - data type to be assigned
def data_type_usable(data1, data2):
    if data1 == "BIT":
        if data2 == "BIT":
            return 1
        else:
            return 0
    elif data1 == "INT":
        if data2 == "BIT":
            return 1
        elif data2 == "INT":
            return 1
        else:
            return 0
    elif data1 == "FLOAT":
        if data2 == "BIT":
            return 1
        elif data2 == "INT":
            return 1
        elif data2 == "FLOAT":
            return 1
        else:
            return 0
    elif data1 == "NVARCHAR":
        if data2 == "BIT":
            return 1
        elif data2 == "INT":
            return 1
        elif data2 == "FLOAT":
            return 1
        elif data2 == "NVARCHAR":
            return 1
        else:
            return 0
    elif data1 == "NTEXT":
        return 1

    return 1

################################################################################
# Recursively browse xml document and update entries in database.
# @param - relative root item to start with
# @param - database to work with
# @return - none
def xtd_database(item, db):
    info = {}

    # Remember columns
    for cname, data in item.items():
        db.update_attribute(item.tag.lower(), cname.lower(), data)

    # Update value
    if item.text and not item.text.isspace():
        db.update_value(item.tag.lower(), item.text)

        # Rememeber elations
    for child in item.getchildren():
        info[child.tag.lower()] = info.get(child.tag.lower(), 0) + 1

        # Recursively look for children
        xtd_database(child, db)

    db.update_relations(item.tag.lower(), info)

################################################################################
# Analyse input and if it is correct print asked output.
# @param - output file to write to
# @param - cmd-line parameters as a dict
# @return - none
def xtd(fin, fout, fval, param):
    """Analyse input and make output for XTD."""
    db = Database(etc = param.get("etc", -1),
                  duplicity = "b" in param,
                  no_columns = "a" in param)
    tree = etree.parse(fin)

    for item in tree.getroot().getchildren():
        xtd_database(item, db)

    # Bonus implementation.
    if "isvalid" in param:
        db2 = Database(etc = param.get("etc", -1),
                       duplicity = "b" in param,
                       no_columns = "a" in param)
        tree2 = etree.parse(fval)

        for item in tree2.getroot().getchildren():
            xtd_database(item, db2)

        if not db.is_subset(db2):
            raise XTDNotValid

    if "header" in param:
        print("--", file=fout, end="")
        print(param["header"], file=fout)
        print("", file=fout)

    if "g" in param:
        db.print_xmlrel(fout)
    else:
        db.print_ddl(fout)

################################################################################
# Print warning msg on stderr if passed and print help
# @param - msg to be printed on stderr (optional)
# @return - none
def print_help(errmsg = ""):
    """Print warning msg on stderr if passed and print help"""
    if errmsg: print(errmsg, file=sys.stderr)

    print("Usage: " + sys.argv[0] + " [OPTION]");
    print("XML2DDL conversion tool.");
    print("  --help             print this simple help");
    print("  --input=FILE       specify input file (UTF-8)");
    print("  --output=FILE      specify output FILE (UTF-8)");
    print("  --header=HEADING   specify header of the output file");
    print("  --etc=NUM          use up to NUM columns");
    print("  -a                 do not generate columns");
    print("  -b                 ignore duplicity (do not use with --etc)");
    print("  -g                 generate XML file only");
    print("Fridolin Pokorny 2012 <fridex.devel@gmail.com>");
    print("Version: 0.1a");

################################################################################
# Process arguments and check for necessary options.
# @param - none
# @return - none
def check_opt():
    """Process arguments and check for necessary options."""

    opts, args = getopt.getopt(sys.argv[1:], "abg", ["help",
                                                     "isvalid=",
                                                     "output=",
                                                     "input=",
                                                     "header=",
                                                     "etc="])
    if args:
        raise XTDCheckArgument("Unknown option : ")

    param = {}
    for option, argument in opts:
        if option == "--isvalid":
            if "isvalid" not in param: param["isvalid"] = argument
            else: raise XTDCheckArgument("Duplicit argument --isvalid!")

        elif option == "--output":
            if "output" not in param: param["output"] = argument
            else: raise XTDCheckArgument("Duplicit argument --output!")

        elif option == "--input":
            if "input" not in param: param["input"] = argument
            else: raise XTDCheckArgument("Duplicit argument --input!")

        elif option == "--help":
            if "help" not in param: param["help"] = "help";
            else: raise XTDCheckArgument("Duplicit argument --help!")

        elif option == "--header":
            if "header" not in param: param["header"] = argument;
            else: raise XTDCheckArgument("Duplicit argument --header!")

        elif option == "--etc":
            try:
                if "etc" not in param: param["etc"] = int(argument);
                else: raise XTDCheckArgument("Duplicit argument --etc!")
            except ValueError:
                    raise XTDCheckArgument("Please enter integer value for --etc!")

            if param["etc"] < 0:
                raise XTDCheckArgument("Negative --etc!")

            if "b" in param:
                raise XTDCheckArgument("--etc and -b option not allowed at the "
                                    "same time!")

        elif option == "-a":
            if "a" not in param: param["a"] = "a";
            else: raise XTDCheckArgument("Duplicit argument -a!")

        elif option == "-b":
            if "b" not in param: param["b"] = "b";
            else: raise XTDCheckArgument("Duplicit argument -b!")

            if "etc" in param:
                raise XTDCheckArgument("-b and --etc option not allowed at the "
                                    "same time!")

        elif option == "-g":
            if "g" not in param: param["g"] = "g";
            else: raise XTDCheckArgument("Duplicit argument -g!")

        else:
            raise XTDCheckArgument("Unknown option: " + option)

    if "help" in param and len(param) > 1:
        raise XTDCheckArgument("Help needed!")

    return param

################################################################################
# Main function. Do what user want!
# @param - none
# @return - none
def main():
    try:
        param = check_opt()

        if "help" not in param:
            try:
                if "input" not in param: fin = sys.stdin
                else: fin = io.open(param["input"], 'r', encoding='utf-8')
            except IOError as err:
                raise XTDIError(err)

            try:
                if "output" not in param: fout = sys.stdout
                else: fout = io.open(param["output"], 'w', encoding='utf-8')
            except IOError as err:
                raise XTDOError(err)

            try:
                if "isvalid" in param:
                    fval = io.open(param["isvalid"], 'r', encoding='utf-8')
                else:
                    fval = {}
            except IOError as err:
                raise XTDIError(err)

            xtd(fin, fout, fval, param)

            if fin != sys.stdin: fin.close()
            if fout != sys.stdout: fout.close()
            if "isvalid" in param: fval.close()

        else:
            print_help()
            del param["help"]
            if len(param) != 0: sys.exit(1);  # No other parameters possible!

        return 0;

    except getopt.GetoptError as exc:
        print_help(exc)
        sys.exit(1)

    except XTDCheckArgument as exc:
        print_help(exc)
        sys.exit(1)

    except XTDNotValid as exc:
        print("Given file is not valid!", file=sys.stderr)
        sys.exit(91)

    except XTDNameError:
        print("Name collision!", file=sys.stderr)
        sys.exit(90)

    except etree.ParseError:
        print("Bad XML input file!\n", file=sys.stderr)
        sys.exit(2)

    except parsers.ExpatError:
        print("Bad XML input file!\n", file=sys.stderr)
        sys.exit(2)

    except XTDOError as err:
        print(err, file=sys.stderr)
        sys.exit(2)

    except XTDIError as err:
        print(err, file=sys.stderr)
        sys.exit(2)

################################################################################

if __name__ == '__main__':
    main()

