package com.elastic.elasticsearch.repository;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.ElasticsearchException;
import co.elastic.clients.elasticsearch._types.query_dsl.MatchQuery;
import co.elastic.clients.elasticsearch._types.query_dsl.Query;
import co.elastic.clients.elasticsearch.core.*;
import co.elastic.clients.elasticsearch.core.bulk.BulkResponseItem;
import co.elastic.clients.elasticsearch.core.search.Hit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

import com.elastic.elasticsearch.entity.Product;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

@Repository
public class ProductRepository {
	@Autowired
	private ElasticsearchClient elasticsearchClient;

	private final String indexName = "products";
	private static final Logger logger=LoggerFactory.getLogger(ProductRepository.class);
	public String createOrUpdateDocument(Product product) throws IOException {

		IndexResponse response = elasticsearchClient
				.index(i -> i.index(indexName).id(product.getId() + "").document(product));
		if (response.result().name().equals("Created")) {
			return new StringBuilder("Document has been successfully created.").toString();
		} else if (response.result().name().equals("Updated")) {
			return new StringBuilder("Document has been successfully updated.").toString();
		}
		return new StringBuilder("Error while performing the operation.").toString();
	}

	public Product getDocumentById(String productId) throws IOException {
		Product product = null;
		GetResponse<Product> response = elasticsearchClient.get(g -> g.index(indexName).id(productId), Product.class);

		if (response.found()) {
			product = response.source();
			System.out.println("Product name " + product.getName());
		} else {
			System.out.println("Product not found");
		}

		return product;
	}

	public String deleteDocumentById(String productId) throws IOException {

		DeleteRequest request = DeleteRequest.of(d -> d.index(indexName).id(productId));

		DeleteResponse deleteResponse = elasticsearchClient.delete(request);
		if (Objects.nonNull(deleteResponse.result()) && !deleteResponse.result().name().equals("NotFound")) {
			return new StringBuilder("Product with id " + deleteResponse.id() + " has been deleted.").toString();
		}
		System.out.println("Product not found");
		return new StringBuilder("Product with id " + deleteResponse.id() + " does not exist.").toString();

	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	public List<Product> searchAllDocuments() throws IOException {

		SearchRequest searchRequest = SearchRequest.of(s -> s.index(indexName));
		SearchResponse searchResponse = elasticsearchClient.search(searchRequest, Product.class);
		List<Hit> hits = searchResponse.hits().hits();
		List<Product> products = new ArrayList<>();
		for (Hit object : hits) {

			System.out.print(((Product) object.source()));
			products.add((Product) object.source());

		}
		return products;
	}

	public List<Product> seList(String key, String fuzziness) throws IOException {
//    	SearchRequest searchRequest =  SearchRequest.of(s -> s.index(indexName)
//    			.query(q->q.match(t->t.field("name").query(key))));
		Query byName = MatchQuery.of(m -> m.field("name").query(key).fuzziness(fuzziness).prefixLength(0))._toQuery();
		Query byDes = MatchQuery.of(m -> m.field("des").fuzziness(fuzziness).prefixLength(0).query(key))._toQuery();
		Query byKey = MatchQuery.of(m -> m.field("key").fuzziness(fuzziness).prefixLength(0).query(key))._toQuery();
		SearchResponse<Product> response = elasticsearchClient.search(
				s -> s.index("products").query(
						q -> q.bool(b -> b.should(byName).should(byDes).should(byKey).minimumShouldMatch("50%"))),
				Product.class);
		List<Hit<Product>> hits = response.hits().hits();
		List<Product> products = new ArrayList<>();
		for (Hit object : hits) {

			System.out.print(((Product) object.source()));
			products.add((Product) object.source());

		}
		return products;
	}

	public void insert() throws ElasticsearchException, IOException {
		BulkRequest.Builder br = new BulkRequest.Builder();
		for (int i = 10000; i < 100000; i++) {
			Product p = new Product();
			p.setId(i + "");
			p.setKey("key " + i);
			p.setName("name " + i);
			p.setDes("des " + i);
			br.operations(op -> op.index(idx -> idx.index(indexName).id(p.getId()).document(p)));
		}
		BulkResponse result = elasticsearchClient.bulk(br.build());
		if (result.errors()) {
		    logger.error("Bulk had errors");
		    for (BulkResponseItem item: result.items()) {
		        if (item.error() != null) {
		            logger.error(item.error().reason());
		        }
		    }
		}
	}
}
