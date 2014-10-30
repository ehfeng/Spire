# queryparser.py
#
# Takes the simpleSQL example and adapts it for use with Hive queries
#
# TODO:
# cast(variable as variabletype)
# group by and having acting up :(
# joins with complicated on statements  
#
from pyparsing import CaselessLiteral, Word, delimitedList, Optional, \
    Combine, Group, nums, alphanums, Forward, oneOf, quotedString, \
    ZeroOrMore, restOfLine, Keyword, SkipTo, OneOrMore

# define base types and operators
E = CaselessLiteral("E")
arithop  = Word( "+-*/", max=1 )   # arithmetic operators
binop = oneOf("= != <> < > >= <= eq ne lt le gt ge", caseless=True)
arithSign = Word("+-",exact=1)
realNum = Combine( Optional(arithSign) + ( Word( nums ) + "." + Optional( Word(nums) )  |
                                                         ( "." + Word(nums) ) ) +
            Optional( E + Optional(arithSign) + Word(nums) ) )
intNum = Combine( Optional(arithSign) + Word( nums ) +
            Optional( E + Optional("+") + Word(nums) ) )

# define SQL tokens and keywords
selectToken = Keyword("select", caseless=True)
fromToken = Keyword("from", caseless=True)
joinToken = Keyword("join", caseless=True)
whereToken = Keyword("where", caseless=True)
groupToken = Keyword("group by", caseless=True)
havingToken = Keyword("having", caseless=True)
asToken = Keyword("as", caseless=True)

caseStart = Keyword("case", caseless=True)
caseEnd = Keyword("end", caseless=True)
distinct_ = Keyword("distinct", caseless=True)
and_ = Keyword("and", caseless=True)
or_ = Keyword("or", caseless=True)
in_ = Keyword("in", caseless=True)
is_ = Keyword("is", caseless=True)
not_ = Keyword("not", caseless=True)
between_ = Keyword("between", caseless=True)
null = Keyword("null", caseless=True)

#query elements
selectStmt = Forward()

colIdent = Word( alphanums + "_$().*[]'" ).setName("column identifier")
columnName = Combine( (caseStart + SkipTo(caseEnd, include=True)) |
                      SkipTo(",", failOn=asToken) | SkipTo(asToken) | SkipTo(fromToken) | SkipTo(havingToken)
              , adjacent=False)

columnNameList = delimitedList( columnName.setResultsName("columns", listAllMatches=True)
				+ Optional(asToken + colIdent) )

tableName = Word( alphanums + "_$." ).setName("table identifier")

whereExpression = Forward()
havingExpression = Forward()

whereColumn = (( caseStart + SkipTo(caseEnd, include=True)) |
               ( Optional("(") + colIdent + Optional("(") + ZeroOrMore(arithop + Optional("(") + colIdent + Optional(")")) + Optional(")") )
              )

columnRval = realNum | intNum | quotedString | whereColumn # need to add support for alg expressions

condition = Group(
  ( whereColumn + binop + columnRval ) |
  ( whereColumn + Optional(not_) + in_ + "(" + delimitedList( columnRval ) + ")" ) |
  ( whereColumn + is_ + Optional(not_) + null ) |
  ( whereColumn + between_ + columnRval + and_ + columnRval) |
  ( "(" + whereExpression + ")" )
  )
whereExpression << condition.setResultsName("where", listAllMatches=True) + ZeroOrMore( ( and_ | or_ ) + whereExpression )
havingExpression << condition.setResultsName("having", listAllMatches=True) + ZeroOrMore( ( and_ | or_ ) + havingExpression )

groupByList = delimitedList( columnName.setResultsName("groupby", listAllMatches=True) )

# define the grammar
selectStmt << ( selectToken +
  ( Optional(distinct_) + columnNameList ) +
  fromToken +
  ( tableName.setResultsName("tables", listAllMatches=True )  | ("(" + selectStmt + Optional(")")) ) +
  ZeroOrMore( SkipTo(joinToken, include=True, failOn=whereToken) +
  			(("(" + selectStmt + Optional(")")) | tableName.setResultsName("tables", listAllMatches=True)) ) +
  Optional(SkipTo(whereToken, include=True, failOn=")") + whereExpression) +
  Optional(SkipTo(groupToken, include=True, failOn=")") + groupByList) + 
  Optional(SkipTo(havingToken, include=True, failOn=")") + havingExpression))

queryToParse = selectStmt

# define Oracle comment format, and ignore them
oracleSqlComment = "--" + restOfLine
queryToParse.ignore( oracleSqlComment )

