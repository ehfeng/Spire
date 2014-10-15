# queryparser.py
#
# Takes the simpleSQL example and adapts it for use with Hive queries
#
# TODO:
# - UDFs (i.e. $DATE(0)) x
# - Column and table aliases x
# - joins (add join keyword)
# - subqueries (recurse selectStmt) x
# - support for expressions in columns (ex. col1 + col2)
# - support for case statements
#
from pyparsing import Literal, CaselessLiteral, Word, Upcase, delimitedList, Optional, \
    Combine, Group, alphas, nums, alphanums, ParseException, Forward, oneOf, quotedString, \
    ZeroOrMore, restOfLine, Keyword, SkipTo, OneOrMore

def test( str ):
    print(str,"->")
    try:
        tokens = simpleSQL.parseString( str )
        print("tokens = ",        tokens)
        print("tokens.columns =", tokens.columns)
        print("tokens.tables =",  tokens.tables)
        print("tokens.where =", tokens.where)
    except ParseException as err:
        print(" "*err.loc + "^\n" + err.msg)
        print(err)
    print()

# define SQL tokens
selectStmt = Forward()
selectToken = Keyword("select", caseless=True)
fromToken   = Keyword("from", caseless=True)

colIdent       = Word( alphas + "$", alphanums + "_$()" ).setName("column identifier")
tableIdent     = Word( alphas, alphanums + "_$" ).setName("table identifier")
columnName     = Group (Upcase( delimitedList( colIdent, ".", combine=True )
                      + Optional(CaselessLiteral("as") + colIdent)) )
columnNameList = Group( delimitedList( columnName ) )
tableName      = Upcase( delimitedList( tableIdent, ".", combine=True ) )

join_ = Keyword("join", caseless=True)
ij_ = Keyword("inner join", caseless=True)
loj_ = Keyword("left outer join", caseless=True)
roj_ = Keyword("right outer join", caseless=True)
foj_ = Keyword("full outer join", caseless=True)

# joins
#ZeroOrMore(SkipTo(join_ | ij_ | loj_ | roj_ | foj_) +
#      (join_ | ij_ | loj_ | roj_ | foj_) + tableName.setResultsName( "tables" )) +

whereExpression = Forward()
and_ = Keyword("and", caseless=True)
or_ = Keyword("or", caseless=True)
in_ = Keyword("in", caseless=True)

E = CaselessLiteral("E")
binop = oneOf("= != < > >= <= eq ne lt le gt ge", caseless=True)
arithSign = Word("+-",exact=1)
realNum = Combine( Optional(arithSign) + ( Word( nums ) + "." + Optional( Word(nums) )  |
                                                         ( "." + Word(nums) ) ) + 
            Optional( E + Optional(arithSign) + Word(nums) ) )
intNum = Combine( Optional(arithSign) + Word( nums ) + 
            Optional( E + Optional("+") + Word(nums) ) )

columnRval = realNum | intNum | quotedString | columnName # need to add support for alg expressions
whereCondition = Group(
    ( columnName + binop + columnRval ) |
    ( columnName + in_ + "(" + delimitedList( columnRval ) + ")" ) |
    ( columnName + in_ + "(" + selectStmt + ")" ) |
    ( columnName + "between" + columnRval + "and" + columnRval) |
    ( "(" + whereExpression + ")" )
    )
whereExpression << whereCondition.setResultsName("where", listAllMatches=True) + ZeroOrMore( ( and_ | or_ ) + whereExpression ) 

# define the grammar
selectStmt      << ( selectToken + 
                   ( '*' | columnNameList.setResultsName("columns", listAllMatches=True) ) + 
                   fromToken + 
                   ( tableName.setResultsName("tables", listAllMatches=True )  | ("(" + selectStmt + ")") ) +
                   Optional(SkipTo(CaselessLiteral("where"), include=True, ignore = ")" + restOfLine) + whereExpression) )

simpleSQL = selectStmt

# define Oracle comment format, and ignore them
oracleSqlComment = "--" + restOfLine
simpleSQL.ignore( oracleSqlComment )

test( "SELECT A as a, B as b, C as c from (select * from INNERTABLE where innerx = 1 and innery = 2) abc where x = 1 and y = 2" )

"""test( "SELECT * from XYZZY, ABC" )
test( "select * from SYS.XYZZY" )
test( "Select A from Sys.dual" )
test( "Select A,B,C from Sys.dual" )
test( "Select A, B, C from Sys.dual" )
test( "Select A, B, C from Sys.dual, Table2   " )
test( "Xelect A, B, C from Sys.dual" )
test( "Select A, B, C frox Sys.dual" )
test( "Select" )
test( "Select &&& frox Sys.dual" )
test( "Select A from Sys.dual where a in ('RED','GREEN','BLUE')" )
test( "Select A from Sys.dual where a in ('RED','GREEN','BLUE') and b in (10,20,30)" )
test( "Select A,b from table1,table2 where table1.id eq table2.id -- test out comparison operators" )"""

"""
Test output:
>pythonw -u simpleSQL.py
SELECT * from XYZZY, ABC ->
tokens =  ['select', '*', 'from', ['XYZZY', 'ABC']]
tokens.columns = *
tokens.tables = ['XYZZY', 'ABC']

select * from SYS.XYZZY ->
tokens =  ['select', '*', 'from', ['SYS.XYZZY']]
tokens.columns = *
tokens.tables = ['SYS.XYZZY']

Select A from Sys.dual ->
tokens =  ['select', ['A'], 'from', ['SYS.DUAL']]
tokens.columns = ['A']
tokens.tables = ['SYS.DUAL']

Select A,B,C from Sys.dual ->
tokens =  ['select', ['A', 'B', 'C'], 'from', ['SYS.DUAL']]
tokens.columns = ['A', 'B', 'C']
tokens.tables = ['SYS.DUAL']

Select A, B, C from Sys.dual ->
tokens =  ['select', ['A', 'B', 'C'], 'from', ['SYS.DUAL']]
tokens.columns = ['A', 'B', 'C']
tokens.tables = ['SYS.DUAL']

Select A, B, C from Sys.dual, Table2    ->
tokens =  ['select', ['A', 'B', 'C'], 'from', ['SYS.DUAL', 'TABLE2']]
tokens.columns = ['A', 'B', 'C']
tokens.tables = ['SYS.DUAL', 'TABLE2']

Xelect A, B, C from Sys.dual ->
^
Expected 'select'
Expected 'select' (0), (1,1)

Select A, B, C frox Sys.dual ->
               ^
Expected 'from'
Expected 'from' (15), (1,16)

Select ->
      ^
Expected '*'
Expected '*' (6), (1,7)

Select &&& frox Sys.dual ->
       ^
Expected '*'
Expected '*' (7), (1,8)

>Exit code: 0
"""