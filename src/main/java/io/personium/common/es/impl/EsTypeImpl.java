/**
 * Personium
 * Copyright 2014-2021 Personium Project Authors
 * - FUJITSU LIMITED
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.personium.common.es.impl;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import co.elastic.clients.elasticsearch._types.ElasticsearchException;
import co.elastic.clients.elasticsearch._types.OpType;
import io.personium.common.es.EsType;
import io.personium.common.es.response.EsClientException;
import io.personium.common.es.response.PersoniumDeleteResponse;
import io.personium.common.es.response.PersoniumGetResponse;
import io.personium.common.es.response.PersoniumIndexResponse;
import io.personium.common.es.response.PersoniumMappingMetaData;
import io.personium.common.es.response.PersoniumMultiSearchResponse;
import io.personium.common.es.response.PersoniumPutMappingResponse;
import io.personium.common.es.response.PersoniumSearchResponse;
import io.personium.common.es.response.impl.PersoniumDeleteResponseImpl;
import io.personium.common.es.response.impl.PersoniumGetResponseImpl;
import io.personium.common.es.response.impl.PersoniumIndexResponseImpl;
import io.personium.common.es.response.impl.PersoniumMappingMetaDataImpl;
import io.personium.common.es.response.impl.PersoniumMultiSearchResponseImpl;
import io.personium.common.es.response.impl.PersoniumNullSearchResponse;
import io.personium.common.es.response.impl.PersoniumPutMappingResponseImpl;
import io.personium.common.es.response.impl.PersoniumSearchResponseImpl;
import io.personium.common.es.util.PersoniumUUID;

/**
 * Class for type operation. After ES6, indices cannot contain multiple types, so this class operates to index which is
 * named with `index.type`.
 */
public class EsTypeImpl implements EsType {

    InternalEsClient esClient;

    private String indexName;
    private String typeName;
    private String routingId;

    public EsTypeImpl(String indexName,
            String typeName,
            String routingId,
            int times,
            int interval,
            InternalEsClient client) {
        this.indexName = indexName;
        this.typeName = typeName;
        this.routingId = routingId;
        this.esClient = client;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getIndexName() {
        return this.indexName;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getType() {
        return this.typeName;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public PersoniumGetResponse get(String id) {
        return this.get(id, true);
    }

    /**
     * {@inheritDoc} If there is not a document specified, this function returns null.
     */
    @Override
    public PersoniumGetResponse get(String id, boolean realtime) {
        try {
            var response = esClient.asyncGet(this.indexName, this.typeName, id, this.routingId, realtime).get();
            if (!response.found()) {
                return null;
            }
            return PersoniumGetResponseImpl.getInstance(response);
        } catch (IOException | InterruptedException e) {
            throw new RuntimeException(e);
        } catch (ExecutionException e) {
            // Elasticsearch throws ElasticsearchException and TransportException in ExecutionException
            Throwable cause = e.getCause();
            if (cause instanceof ElasticsearchException) {
                throw EsClientException.convertException((ElasticsearchException) cause);
            }
            throw new RuntimeException(e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public PersoniumIndexResponse create(@SuppressWarnings("rawtypes") final Map data) {
        String id = PersoniumUUID.randomUUID();
        return this.create(id, data);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public PersoniumIndexResponse create(final String id, final Map data) {
        try {
            // for back compability. Add type property to data.
            var typeAddedData = new HashMap<String, Object>(data);
            if (!typeAddedData.containsKey("type")) {
                typeAddedData.put("type", this.typeName);
            }
            var response = esClient.asyncIndex(this.indexName, this.typeName, id, this.routingId, typeAddedData,
                    OpType.Create, null).get();
            return PersoniumIndexResponseImpl.getInstance(response);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } catch (ExecutionException e) {
            // Elasticsearch throws ElasticsearchException and TransportException in ExecutionException
            Throwable cause = e.getCause();
            if (cause instanceof ElasticsearchException) {
                throw EsClientException.convertException((ElasticsearchException) cause);
            }
            throw new RuntimeException(e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public PersoniumIndexResponse update(String id, final Map data, long version) {
        try {
            // for back compability. Add type property to data.
            var typeAddedData = new HashMap<String, Object>(data);
            if (!typeAddedData.containsKey("type")) {
                typeAddedData.put("type", this.typeName);
            }

            SeqNoPrimaryTerm seqNoPrimaryTerm = null;

            if (version != -1) {
                // optimistic lock
                var prev = esClient.asyncGet(this.indexName, this.typeName, id, this.routingId, true, version).get();
                seqNoPrimaryTerm = new SeqNoPrimaryTerm(prev.seqNo(), prev.primaryTerm());
            }
            var response = esClient.asyncIndex(this.indexName, this.typeName, id, this.routingId, typeAddedData,
                    OpType.Index, seqNoPrimaryTerm).get();
            return PersoniumIndexResponseImpl.getInstance(response);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof ElasticsearchException) {
                throw EsClientException.convertException((ElasticsearchException) cause);
            }
            throw new RuntimeException(e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public PersoniumIndexResponse update(String id, @SuppressWarnings("rawtypes") final Map data) {
        return this.update(id, data, -1);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public PersoniumSearchResponse search(final Map<String, Object> query) {
        try {
            var response = esClient.asyncSearch(this.indexName, this.typeName, this.routingId, query).get();
            return PersoniumSearchResponseImpl.getInstance(response);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } catch (ExecutionException e) {
            // Elasticsearch throws ElasticsearchException and TransportException in ExecutionException
            Throwable cause = e.getCause();
            if (cause instanceof ElasticsearchException) {
                var ex = EsClientException.convertException((ElasticsearchException) cause);
                if (ex instanceof EsClientException.EsIndexMissingException) {
                    return new PersoniumNullSearchResponse();
                } else {
                    throw EsClientException.wrapException("unknown property was appointed.",
                         (ElasticsearchException) cause);
                }
            }
            throw new RuntimeException(e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public PersoniumMultiSearchResponse multiSearch(List<Map<String, Object>> queryList) {
        try {
            var response = esClient.asyncMultiSearch(this.indexName, this.typeName, this.routingId, queryList).get();
            return PersoniumMultiSearchResponseImpl.getInstance(response);
        } catch (IOException | InterruptedException e) {
            throw new RuntimeException(e);
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof ElasticsearchException) {
                throw EsClientException.convertException((ElasticsearchException) cause);
            }
            throw new RuntimeException(e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public PersoniumDeleteResponse delete(String docId) {
        return this.delete(docId, -1);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public PersoniumDeleteResponse delete(String docId, long version) {
        try {
            var response = esClient.asyncDelete(this.indexName, this.typeName, docId, this.routingId, version).get();
            return PersoniumDeleteResponseImpl.getInstance(response);
        } catch (IOException | InterruptedException e) {
            throw new RuntimeException(e);
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof ElasticsearchException) {
                throw EsClientException.convertException((ElasticsearchException) cause);
            }
            throw new RuntimeException(e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public PersoniumMappingMetaData getMapping() {
        try {
            var response = esClient.getMapping(this.indexName, this.typeName);
            return PersoniumMappingMetaDataImpl.getInstance(response);
        } catch (IOException e) {
            throw new RuntimeException(e);
        } catch (ElasticsearchException e) {
            throw EsClientException.convertException(e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public PersoniumPutMappingResponse putMapping(Map<String, Object> mappings) {
        try {
            var response = esClient.putMapping(this.indexName, this.typeName, mappings);
            return PersoniumPutMappingResponseImpl.getInstance(response);
        } catch (IOException e) {
            throw new RuntimeException(e);
        } catch (ElasticsearchException e) {
            throw EsClientException.convertException(e);
        }
    }

}
