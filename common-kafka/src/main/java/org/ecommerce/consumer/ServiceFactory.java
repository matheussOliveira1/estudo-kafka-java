package org.ecommerce.consumer;

public interface ServiceFactory<T> {

    ConsumerService<T> create() throws Exception;

}
