from queryparser import queryToParse

def test( str ):
    #print(str,"->")
    try:
        tokens = queryToParse.parseString( str )
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

test( "SELECT A as a, B as b, C as c from (select * from INNERTABLE where innerx = 1 and innery = 2) abc where x = 1 and y = 2" )
test( "Select A from (select * from Sys.dual where a in ('RED','GREEN','BLUE') and b in (10,20,30))" )

test( """SELECT A as cola, B as colb 
        from (select a from 
            (select * from table1 where innerwhere="subsubquery") a
          join table2 b on a.col1=b.col1) a 
        left outer join table3 b 
        on a.col1 = b.col1 where test1=1""")

