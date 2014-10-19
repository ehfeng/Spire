# queryparser.py
#
# Takes the simpleSQL example and adapts it for use with Hive queries
#
# TODO:
# - UDFs (i.e. $DATE(0)) x
# - Column and table aliases x
# - joins (add join keyword) x
# - subqueries (recurse selectStmt) x
# - support for arithmetic expressions in columns (ex. col1 + col2) x
# - support for if/case/etc. statements x
# - group by / having x
#
from pyparsing import Literal, CaselessLiteral, Word, Upcase, delimitedList, Optional, \
    Combine, Group, alphas, nums, alphanums, ParseException, Forward, oneOf, quotedString, \
    ZeroOrMore, restOfLine, Keyword, SkipTo, OneOrMore

# define base types and operators
E = CaselessLiteral("E")
arithop  = Word( "+-*/", max=1 )   # arithmetic operators
binop = oneOf("= != < > >= <= eq ne lt le gt ge", caseless=True)
arithSign = Word("+-",exact=1)
realNum = Combine( Optional(arithSign) + ( Word( nums ) + "." + Optional( Word(nums) )  |
                                                         ( "." + Word(nums) ) ) +
            Optional( E + Optional(arithSign) + Word(nums) ) )
intNum = Combine( Optional(arithSign) + Word( nums ) +
            Optional( E + Optional("+") + Word(nums) ) )

# define SQL tokens
selectStmt = Forward()
selectToken = Keyword("select", caseless=True)
fromToken = Keyword("from", caseless=True)
asToken = Keyword("as", caseless=True)

colIdent = Upcase(Word( alphanums + "_$()." ).setName("column identifier"))
columnName = Combine( ( CaselessLiteral("case when") + SkipTo(CaselessLiteral("end"), include=True) ) |
               ( Optional("(") + colIdent + Optional("(") + ZeroOrMore((arithop|binop) + Optional("(") + colIdent + Optional(")")) + Optional(")") )
             , adjacent=False )

columnNameList = delimitedList( columnName.setResultsName("columns", listAllMatches=True)
				+ Optional(asToken + colIdent) )

tableName = Upcase(Word( alphanums + "_$." ).setName("table identifier"))

whereExpression = Forward()
havingExpression = Forward()
and_ = Keyword("and", caseless=True)
or_ = Keyword("or", caseless=True)
in_ = Keyword("in", caseless=True)

whereColumn = (( CaselessLiteral("case when") + SkipTo(CaselessLiteral("end"), include=True)) |
               ( Optional("(") + colIdent + Optional("(") + ZeroOrMore(arithop + Optional("(") + colIdent + Optional(")")) + Optional(")") )
              )

columnRval = realNum | intNum | Upcase(quotedString) | whereColumn # need to add support for alg expressions

condition = Group(
  ( whereColumn + binop + columnRval ) |
  ( whereColumn + in_ + "(" + delimitedList( columnRval ) + ")" ) |
  ( whereColumn + in_ + "(" + selectStmt + ")" ) |
  ( whereColumn + "between" + columnRval + "and" + columnRval) |
  ( "(" + whereExpression + ")" )
  )
whereExpression << condition.setResultsName("where", listAllMatches=True) + ZeroOrMore( ( and_ | or_ ) + whereExpression )
havingExpression << condition.setResultsName("having", listAllMatches=True) + ZeroOrMore( ( and_ | or_ ) + havingExpression )

groupByList = delimitedList( columnName.setResultsName("groupby", listAllMatches=True) )

# define the grammar
selectStmt << ( selectToken +
  ( Optional(colIdent + '.') + '*' | columnNameList ) +
  fromToken +
  ( tableName.setResultsName("tables", listAllMatches=True )  | ("(" + selectStmt + Optional(")")) ) +
  ZeroOrMore( SkipTo(CaselessLiteral("join"), include=True, failOn="where") +
  			(("(" + selectStmt + Optional(")")) | tableName.setResultsName( "tables", listAllMatches=True )) ) +
  Optional(SkipTo(CaselessLiteral("where"), include=True, failOn=")") + whereExpression) +
  Optional(SkipTo(CaselessLiteral("group by"), include=True, failOn=")") + groupByList) + 
  Optional(SkipTo(CaselessLiteral("having"), include=True, failOn=")") + havingExpression))

queryToParse = selectStmt

# define Oracle comment format, and ignore them
oracleSqlComment = "--" + restOfLine
queryToParse.ignore( oracleSqlComment )

print queryToParse.parseString("select x+y/z from test.table where x + y/z > 1")

