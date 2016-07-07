import pyes
conn = pyes.ES(['127.0.0.1:9200'])
search = pyes.query.MatchAllQuery().search(bulk_read=1000)
#hits = conn.search(search, 'forum', 'CHAT', scan=True, scroll="30m", model=lambda _,hit: hit)
hits = conn.search(search, 'forum', 'CHAT')
#print hits.count
for hit in hits:
     #print hit
	conn.index(hit['_source'], 'chat', 'CHAT', hit['_id'], bulk=True)
#conn.flush()
