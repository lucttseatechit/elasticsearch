package com.elastic.elasticsearch.api;

import java.io.IOException;
import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.elastic.elasticsearch.entity.Product;
import com.elastic.elasticsearch.repository.ProductRepository;

import co.elastic.clients.elasticsearch._types.ElasticsearchException;

@RestController
public class ProductApi {
	@Autowired
	private ProductRepository repository;
	@GetMapping("/product")
	public ResponseEntity<?> get(@RequestParam("string") String string) throws IOException{
		if(string==null || "".equals(string)) {
			return ResponseEntity.ok(repository.searchAllDocuments());
		}else {
			List<Product> list;
			list= repository.seList(string,"AUTO");
			if(list.isEmpty()) {
				list=repository.seList(string,"1");
			}
			if(list.isEmpty()) {
				list=repository.seList(string, "2");
			}
			return ResponseEntity.ok(list);
		}
	}
	@PostMapping("/product")
	public ResponseEntity<?> add(Product product) throws IOException{
		return ResponseEntity.ok(repository.createOrUpdateDocument(product));
	}
	@PostMapping("/bulk")
	public ResponseEntity<?> add() throws ElasticsearchException, IOException {
		repository.insert();
		return ResponseEntity.ok("oke rồi");
	}
}
