package com.elastic.elasticsearch.api;

import java.io.IOException;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.elastic.elasticsearch.entity.Product;
import com.elastic.elasticsearch.repository.ProductRepository;

@RestController
public class ProductApi {
	@Autowired
	private ProductRepository repository;
	@GetMapping("/product")
	public ResponseEntity<?> get(@RequestParam("string") String string) throws IOException{
		if(string==null || "".equals(string)) {
			return ResponseEntity.ok(repository.searchAllDocuments());
		}else {
			return ResponseEntity.ok(repository.seList(string));
		}
	}
	@PostMapping("/product")
	public ResponseEntity<?> add(Product product) throws IOException{
		return ResponseEntity.ok(repository.createOrUpdateDocument(product));
	}
}
