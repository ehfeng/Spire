# queryparser.py
#
# Takes the simpleSQL example and adapts it for use with Hive queries
#
# TODO:
# - UDFs (i.e. $DATE(0)) x
# - Column and table aliases x
# - joins (add join keyword) x
# - subqueries (recurse selectStmt) x
# - support for expressions in columns (ex. col1 + col2)
# - support for case statements
# - group by
#
from pyparsing import Literal, CaselessLiteral, Word, Upcase, delimitedList, Optional, \
    Combine, Group, alphas, nums, alphanums, ParseException, Forward, oneOf, quotedString, \
    ZeroOrMore, restOfLine, Keyword, SkipTo, OneOrMore

def test( str ):
    #print(str,"->")
    try:
        tokens = simpleSQL.parseString( str )
        print("tokens = ",        tokens)
        print()
        print("tokens.columns =", tokens.columns)
        print("tokens.tables =",  tokens.tables)
        print("tokens.where =", tokens.where)
    except ParseException as err:
        print(" "*err.loc + "^\n" + err.msg)
        print(err)
    print()
    print()
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
                   ( tableName.setResultsName("tables", listAllMatches=True )  | ("(" + selectStmt + Optional(")")) ) +
                   ZeroOrMore( SkipTo(CaselessLiteral("join"), include=True, failOn="where") + 
                   (("(" + selectStmt + Optional(")")) | tableName.setResultsName( "tables", listAllMatches=True )) ) + 
                   Optional(SkipTo(CaselessLiteral("where"), include=True, failOn=")") + whereExpression) )

simpleSQL = selectStmt

# define Oracle comment format, and ignore them
oracleSqlComment = "--" + restOfLine
simpleSQL.ignore( oracleSqlComment )

test( "SELECT A as a, B as b, C as c from (select * from INNERTABLE where innerx = 1 and innery = 2) abc where x = 1 and y = 2" )
test( "Select A from (select * from Sys.dual where a in ('RED','GREEN','BLUE') and b in (10,20,30))" )

test( """SELECT A as cola, B as colb 
        from (select a from 
            (select * from table1 where innerwhere="subsubquery") a
          join table2 b on a.col1=b.col1) a 
        left outer join table3 b 
        on a.col1 = b.col1 where test1=1
        group by A, B """)





