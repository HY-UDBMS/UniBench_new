#!/bin/sh

## Relational

arangoimp --file "./Unibench/CSV_Customer/person.csv" --type csv --translate "personId=_key" --separator "|" --collection "Customer" --server.username root  --server.password "123"  --create-collection true --threads 8

arangoimp --file "./Unibench/KeyValue_Rating/rating.csv" --type csv  --separator "|" --collection "Feedback" --server.username root  --server.password "123"    --create-collection true --convert false --threads 8

arangoimp --file "./Unibench/CSV_Vendor/vendor.csv" --type csv  --translate "id=_key"  --collection "Vendor" --server.username root  --server.password "123"    --create-collection true --threads 8

arangoimp --file "./Unibench/CSV_Product/product.csv" --type csv --translate "productId=_key" --collection "Product" --server.username root  --server.password "123"    --create-collection true --threads 8


## JSON
arangoimp --file "./Unibench/JSON_Order/order.json" --type jsonl --translate "id=_key" --collection "Order" --server.username root  --server.password "123"    --create-collection true --convert false --threads 8


## Graph

arangoimp --file "./Unibench/Graph_SocialNetwork/Post/post.csv" --type csv --separator "|" --translate "postId=_key" --collection "Post"  --server.username root  --server.password "123" --create-collection true  --threads 8

arangoimp --file "./Unibench/Graph_SocialNetwork/Tag/tag.csv" --type csv --separator "|" --translate "id=_key" --collection "Tag"  --server.username root  --server.password "123" --create-collection true  --threads 8

arangoimp --file "./Unibench/Graph_SocialNetwork/PersonKnowsPerson/person_knows_person.csv" --type csv --separator "|" --translate "personIdsrc=_from" --translate "personIddst=_to" --collection "KnowsGraph" --from-collection-prefix Customer --to-collection-prefix Customer --server.username root  --server.password "123" --create-collection true --create-collection-type edge --threads 8

arangoimp --file "./Unibench/Graph_SocialNetwork/PersonKnowsPerson_Random/person_knows_person.csv" --type csv --separator "|" --translate "personIdsrc=_from" --translate "personIddst=_to" --collection "KnowsGraph" --from-collection-prefix Customer --to-collection-prefix Customer --server.username root  --server.password "123" --create-collection-type edge --threads 8

arangoimp --file "./Unibench/Graph_SocialNetwork/PersonHasPost/person_has_post.csv" --type csv --separator "|" --translate "personId=_from" --translate "postId=_to" --collection "PersonHasPost" --from-collection-prefix Customer --to-collection-prefix Post --server.username root  --server.password "123" --create-collection true --create-collection-type edge --threads 8

arangoimp --file "./Unibench/Graph_SocialNetwork/PostHasTag/post_has_tag.csv" --type csv --separator "|" --translate "postId=_from" --translate "tagId=_to" --collection "PostHasTag" --from-collection-prefix Post --to-collection-prefix Tag --server.username root  --server.password "123" --create-collection true --create-collection-type edge --threads 8

## RDF
arangoimp --file "./Unibench/RDF_Product_nodes/RDF_product_nodes.json" --type jsonl --separator "|" --collection "RDF_Product_nodes"  --server.username root  --server.password "123" --create-collection true  --threads 8

arangoimp --file "./Unibench/RDF_Product_edges/RDF_product_edges.json" --type jsonl --separator "|" --collection "RDF_Product_edges"  --from-collection-prefix RDF_Product_nodes --to-collection-prefix RDF_Product_nodes  --server.username root  --server.password "123" --create-collection true  --create-collection-type edge --threads 8