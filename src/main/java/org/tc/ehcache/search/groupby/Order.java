package org.tc.ehcache.search.groupby;

import java.io.Serializable;

/**
 * Simple Order class that can be searched on by the Search API
 */

public class Order implements Serializable{

	private static final long serialVersionUID = 1L;
	private final int orderId;
	private final int orderPrice;
	private final String city;
	private String customerName;
	
    public Order(int orderId, int orderPrice, String city, String customerName) {
        this.orderId = orderId;
        this.orderPrice = orderPrice;
        this.city = city;
        this.customerName = customerName;
    }

	public String getCustomerName() {
		return customerName;
	}

	public int getOrderId() {
		return orderId;
	}

	public int getOrderPrice() {
		return orderPrice;
	}

	public String getCity() {
		return city;
	}


}

