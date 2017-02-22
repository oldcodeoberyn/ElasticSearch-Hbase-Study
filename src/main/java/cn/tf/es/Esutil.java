package cn.tf.es;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.annotation.PostConstruct;

import org.apache.commons.lang.StringUtils;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.highlight.HighlightField;
import org.springframework.stereotype.Component;

@Component
public class Esutil
{
    private Client client = null;

    @PostConstruct
    public void init()
    {
        createClient();
    }

    private void createClient()
    {
        Settings settings = Settings.settingsBuilder().put( "cluster.name", "my-es" ).build();

        try
        {
            client = TransportClient.builder().settings( settings ).build()
                .addTransportAddress(
                    new InetSocketTransportAddress( InetAddress.getByName( "euca-10-157-65-152" ), 9300 ) )
                .addTransportAddress(
                    new InetSocketTransportAddress( InetAddress.getByName( "euca-10-157-66-195" ), 9300 ) );
        }
        catch( UnknownHostException e )
        {
            e.printStackTrace();
        }
    }

    public String addIndex( String index, String type, Doc doc )
    {
        HashMap<String, Object> hashMap = new HashMap<String, Object>();
        hashMap.put( "id", doc.getId() );
        hashMap.put( "title", doc.getTitle() );
        hashMap.put( "describe", doc.getDescribe() );
        hashMap.put( "author", doc.getAuthor() );

        IndexResponse response = client.prepareIndex( index, type ).setSource( hashMap ).execute().actionGet();
        return response.getId();
    }

    public Map<String, Object> search( String key, String index, String type, int start, int row )
    {
        SearchRequestBuilder builder = client.prepareSearch( index );
        builder.setTypes( type );
        builder.setFrom( start );
        builder.setSize( row );
        // 设置高亮字段名称
        builder.addHighlightedField( "title" );
        builder.addHighlightedField( "describe" );
        // 设置高亮前缀
        builder.setHighlighterPreTags( "<font color='red' >" );
        // 设置高亮后缀
        builder.setHighlighterPostTags( "</font>" );
        builder.setSearchType( SearchType.DFS_QUERY_THEN_FETCH );
        if( StringUtils.isNotBlank( key ) )
        {
            // builder.setQuery(QueryBuilders.termQuery("title",key));
            builder.setQuery( QueryBuilders.multiMatchQuery( key, "title", "describe" ) );
        }
        builder.setExplain( true );
        SearchResponse searchResponse = builder.get();

        SearchHits hits = searchResponse.getHits();
        long total = hits.getTotalHits();
        System.out.println( "hit total: " + total );
        Map<String, Object> map = new HashMap<String, Object>();
        SearchHit[] hits2 = hits.getHits();
        map.put( "count", total );
        List<Map<String, Object>> list = new ArrayList<Map<String, Object>>();
        for( SearchHit searchHit : hits2 )
        {
            Map<String, HighlightField> highlightFields = searchHit.getHighlightFields();
            HighlightField highlightField = highlightFields.get( "title" );
            Map<String, Object> source = searchHit.getSource();
            if( highlightField != null )
            {
                Text[] fragments = highlightField.fragments();
                String name = "";
                for( Text text : fragments )
                {
                    name += text;
                }
                source.put( "title", name );
            }
            HighlightField highlightField2 = highlightFields.get( "describe" );
            if( highlightField2 != null )
            {
                Text[] fragments = highlightField2.fragments();
                String describe = "";
                for( Text text : fragments )
                {
                    describe += text;
                }
                source.put( "describe", describe );
            }
            list.add( source );
        }
        map.put( "dataList", list );
        return map;
    }

}
