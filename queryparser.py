# queryparser.py
#
# Takes the simpleSQL example and adapts it for use with Hive queries
#
# TODO:
# - UDFs (i.e. $DATE(0)) x
# - Column and table aliases x
# - joins (add join keyword) X
# - subqueries (recurse selectStmt) x
# - support for expressions in columns (ex. col1 + col2)
# - support for case statements
#
from pyparsing import Literal, CaselessLiteral, Word, Upcase, delimitedList, Optional, \
    Combine, Group, alphas, nums, alphanums, ParseException, Forward, oneOf, quotedString, \
    ZeroOrMore, restOfLine, Keyword, SkipTo, OneOrMore

# define SQL tokens
selectStmt = Forward()
selectToken = Keyword("select", caseless=True)
fromToken = Keyword("from", caseless=True)

colIdent = Word( alphas + "$", alphanums + "_$()" ).setName("column identifier")
tableIdent = Word( alphas, alphanums + "_$" ).setName("table identifier")
columnName = Group (Upcase( delimitedList( colIdent, ".", combine=True )
                      + Optional(CaselessLiteral("as") + colIdent)) )
columnNameList = Group( delimitedList( columnName ) )
tableName = Upcase( delimitedList( tableIdent, ".", combine=True ) )

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
selectStmt << ( selectToken +
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





