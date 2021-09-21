from gremlin_python.driver import client, serializer, protocol

VERTICES = [
    "g.addV('PERSON').property('id', 'P1').property('name', 'Tim Cook').property('title', 'CEO')",
    "g.addV('PERSON').property('id', 'P2').property('name', 'Jonathan Ive').property('title', 'Chief Design Officer')",
    "g.addV('COMPANY').property('id', 'C1').property('name', 'Apple').property('location', 'California, USA')",
    "g.addV('SKILL').property('id', 'S1').property('name', 'Leadership')",
    "g.addV('SKILL').property('id', 'S2').property('name', 'Design')",
    "g.addV('SKILL').property('id', 'S3').property('name', 'Innovation')"
]

EDGES = [
    "g.V('P1').addE('manages').to(g.V('P2'))",
    "g.V('P2').addE('managed by').to(g.V('P1'))",
    "g.V('P1').addE('works for').to(g.V('C1'))",
    "g.V('P2').addE('works for').to(g.V('C1'))",
    "g.V('P1').addE('competent in').to(g.V('S1'))",
    "g.V('P2').addE('competent in').to(g.V('S1'))",
    "g.V('P2').addE('competent in').to(g.V('S2'))",
    "g.V('P2').addE('competent in').to(g.V('S3'))"
]

def cleanup_graph(gremlin_client):
    callback = gremlin_client.submitAsync("g.V().drop()")
    if callback.result() is not None:
        print("Cleaned up the graph!")

def insert_vertices(gremlin_client):
    for vertex in VERTICES:
        callback = gremlin_client.submitAsync(vertex)
        if callback.result() is not None:
            print("Inserted this vertex:\n{0}".format(callback.result().one()))
        else:
            print("Something went wrong with this query: {0}".format(vertex))

def insert_edges(gremlin_client):
    for edge in EDGES:
        callback = gremlin_client.submitAsync(edge)
        if callback.result() is not None:
            print("Inserted this edge:\n{0}".format(callback.result().one()))
        else:
            print("Something went wrong with this query:\n{0}".format(edge))

def handler():
    # Initialise client
    print('Initialising client...')
    gremlin_client = client.Client('wss://jtacosmos.gremlin.cosmosdb.azure.com:443/','g', 
            username="/dbs/revAttrDB/colls/graphCollection", 
            password="SECRET_KEY",
            message_serializer= serializer.GraphSONSerializersV2d0()
        )
    print('Client initialised!')

    # Purge graph
    cleanup_graph(gremlin_client)

    # Insert vertices (i.e. nodes)
    insert_vertices(gremlin_client)

    # Insert edges (i.e. nodes)
    insert_edges(gremlin_client)

    print('Finished!')

if __name__ == '__main__':
    handler()
