import re

import sqlparse

from main import Query

ok = {'select':0, 'create':0, 'use':0, 'drop':0, 'desc':0,
	'show':0, 'set':0, 'insert':0, 'describe':0, 'from':0, 'alter':0}
extra={}
weirdness=[]

for statement in Query.search(size=10000)['hits']['hits']:
	if len(statement['_source']['statement'].strip()) == 0:
		break
	for query in sqlparse.parse(statement['_source']['statement'].strip()):
		if query.to_unicode():
			if query.to_unicode().lower()[:7] == 'select(':
				query = sqlparse.parse(
					re.sub(r'^select\(', 'SELECT (', query.to_unicode(), flags=re.IGNORECASE).strip())[0]
			elif query.to_unicode().strip() == ';':
				break
			try:
				keyword = query.token_next_by_type(0, sqlparse.tokens.Token.Keyword).to_unicode().lower()
				if keyword in ok.keys():
					ok[keyword]+=1
				else:
					weirdness.append(statement['_id'])
					if extra.has_key(query.tokens[0].to_unicode().lower()):
						extra[query.tokens[0].to_unicode().lower()] += 1
					else:
						extra[query.tokens[0].to_unicode().lower()] = 1
			except:
				print "weird error"
				print statement

def process_select_query():
	"""
	"""
	return None

print ok
print extra
print weirdness

# non-select queries
# drop, show, create table, show, use, desc, alter table

# Need to account for: comments

# {
# 	comments: [comments]
# 	select: [column names],
# 	from: [table names],
# 	where: [where statements],
# 	group_by: [group by statements]
# }

# select, from, where, group by, order by, sort by,
# distribute by, cluster by, sort by, limit, union all,
# (left|right) (inner|outer) join