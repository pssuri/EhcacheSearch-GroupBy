package org.tc.ehcache.search.groupby;

import java.io.IOException;
import java.util.List;

import net.sf.ehcache.Cache;
import net.sf.ehcache.CacheManager;
import net.sf.ehcache.Ehcache;
import net.sf.ehcache.Element;
import net.sf.ehcache.config.CacheConfiguration;
import net.sf.ehcache.config.Configuration;
import net.sf.ehcache.config.SearchAttribute;
import net.sf.ehcache.config.Searchable;
import net.sf.ehcache.search.Attribute;
import net.sf.ehcache.search.Query;
import net.sf.ehcache.search.Result;
import net.sf.ehcache.search.Results;
import net.sf.ehcache.search.SearchException;
import net.sf.ehcache.search.aggregator.Aggregators;
import net.sf.ehcache.search.attribute.AttributeExtractor;
import net.sf.ehcache.search.attribute.AttributeExtractorException;

/**
 * Sample app briefly showing groupby feature of the search in Ehcache. This has
 * been written against the latest snapshot build so it can become outdated at
 * any time
 * 
 * 
 * With Ehcache 2.6 and higher, query results may be grouped according to
 * specified attributes by adding an addGroupBy clause to the query, which takes
 * as parameters the attributes to group by. The GroupBy feature provides the
 * option to group results in the same way that you can with an SQL "GROUP BY"
 * clause. In SQL, you can use a SELECT command with a GROUP BY clause to group
 * all rows that have identical values in a specified column, or combination of
 * columns, into a single row. You can also find the aggregate value for each
 * group of column values.
 * 
 * @author psuri
 * 
 */

public class EhcacheSearchGroupBy {
	private CacheManager cacheManager;
	private Ehcache cache;

	public EhcacheSearchGroupBy() {
		initializeCache();
	}

	private void initializeCache() {

		Configuration cacheManagerConfig = new Configuration();

		CacheConfiguration cacheConfig = new CacheConfiguration("test", -1)
				.eternal(true);
		Searchable searchable = new Searchable();
		cacheConfig.addSearchable(searchable);

		searchable.addSearchAttribute(new SearchAttribute().name("orderId")
				.expression("value.getOrderId()"));
		// By Expression
		searchable.addSearchAttribute(new SearchAttribute().name("orderPrice")
				.expression("value.getOrderPrice()"));
		searchable.addSearchAttribute(new SearchAttribute().name("city")
				.expression("value.getCity()"));

		searchable.addSearchAttribute(new SearchAttribute()
				.name("customerName").className(
						NameAttributeExtractor.class.getName()));

		cacheManagerConfig.addCache(cacheConfig);

		cacheManager = new CacheManager(cacheManagerConfig);
		cache = (Cache) cacheManager.getEhcache("test");
		
	}

	public void runTests() throws IOException {

		loadCache();

		Attribute<Integer> orderPrice = cache.getSearchAttribute("orderPrice");
		Attribute<String> name = cache.getSearchAttribute("customerName");
		Attribute<String> city = cache.getSearchAttribute("city");

		// Sum of the order price and grouped by Name and City
		Query query = cache.createQuery();
		try {

			query.includeAttribute(name);
			query.includeAttribute(city);
			query.includeAggregator(Aggregators.sum(orderPrice));
			query.addGroupBy(name, city);

			Results results = query.execute();
			System.out
					.println("Has Aggregators----" + results.hasAggregators());

			for (Result result : results.all()) {
				final List<Object> aggregatorResults = result
						.getAggregatorResults();
				final String attributeResults = result.getAttribute(name);
				final String cityName = result.getAttribute(city);
				System.out
						.print("Sum of the order price and grouped by Name and City--"
								+ attributeResults + "," + cityName + ",");
				for (Object aggregatorResult : aggregatorResults) {
					Long res = (Long) aggregatorResult;
					System.out.println(res);
				}
			}

			// Average of the order price and grouped by Person
			Query query1 = cache.createQuery();

			query1.includeAttribute(name);
			query1.includeAggregator(Aggregators.average(orderPrice));
			query1.addGroupBy(name);

			Results results1 = query1.execute();

			for (Result result1 : results1.all()) {
				final List<Object> aggregatorResults1 = result1
						.getAggregatorResults();
				final String attributeResults = result1.getAttribute(name);
				System.out
						.print("Average of the order price and grouped by Person--"
								+ attributeResults + ",");
				for (Object aggregatorResult : aggregatorResults1) {
					Float res = (Float) aggregatorResult;
					System.out.println(res);
				}
			}

			// Sum of the order price and grouped by City
			Query query2 = cache.createQuery();

			query2.includeAttribute(city);
			query2.includeAggregator(Aggregators.sum(orderPrice));
			query2.addGroupBy(city);

			Results results2 = query2.execute();

			for (Result result : results2.all()) {
				final List<Object> aggregatorResults = result
						.getAggregatorResults();
				final String cityName = result.getAttribute(city);
				System.out.print("Sum of the order price and grouped by City--"
						+ cityName + ",");
				for (Object aggregatorResult : aggregatorResults) {
					Long res = (Long) aggregatorResult;
					System.out.println(res);
				}
			}

		} catch (SearchException e) {
			e.printStackTrace();
		}
	}

	private void loadCache() {

		// Sample Order SQL Table Information is populated in the Cache
		// O_Id OrderCity OrderPrice Customer
		// 1 NY 100 Alpha
		// 2 SFO 160 Beta
		// 3 NY 70 Alpha
		// 4 MI 30 John
		// 5 AZ 200 Bravo
		// 6 LA 100 Sam

		cache.put(new Element(1, new Order(1, 100, "NY", "Alpha")));
		cache.put(new Element(2, new Order(2, 160, "SFO", "Romeo")));
		cache.put(new Element(3, new Order(3, 70, "NY", "Alpha")));
		cache.put(new Element(4, new Order(4, 30, "MI", "John")));
		cache.put(new Element(5, new Order(5, 200, "AZ", "Romeo")));
		cache.put(new Element(6, new Order(6, 200, "LA", "Sam")));
		cache.put(new Element(7, new Order(7, 50, "NY", "John")));
		cache.put(new Element(8, new Order(8, 60, "SFO", "Beta")));
		cache.put(new Element(9, new Order(9, 80, "NY", "Alpha")));
		cache.put(new Element(10, new Order(10, 10, "MI", "John")));
		cache.put(new Element(11, new Order(11, 100, "AZ", "Bravo")));
		cache.put(new Element(12, new Order(12, 300, "LA", "Sam")));

	}

	public static void main(String[] args) throws IOException {
		new EhcacheSearchGroupBy().runTests();
	}

	public static class NameAttributeExtractor implements AttributeExtractor {

		/**
		 * Implementing the AttributeExtractor Interface and passing it in
		 * allows you to create very efficient and specific attribute extraction
		 * for performance sensitive code
		 */

		@Override
		public Object attributeFor(Element arg0, String arg1)
				throws AttributeExtractorException {
			return ((Order) arg0.getValue()).getCustomerName();
		}
	}
}
