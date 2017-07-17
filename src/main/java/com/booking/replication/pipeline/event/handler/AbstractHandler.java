package com.booking.replication.pipeline.event.handler;


import com.booking.replication.applier.ApplierException;
import com.booking.replication.schema.exception.TableMapException;

import java.io.IOException;

/**
 * Created by edmitriev on 7/12/17.
 */
public interface AbstractHandler<E> {
    void apply(E event, long xid) throws ApplierException, EventHandlerApplyException, TableMapException, IOException;

    void handle(E event) throws TransactionException;
}
