
package cascadingtutorial;

import cascading.flow.Flow;
import cascading.flow.FlowConnector;
import cascading.operation.Identity;
import cascading.operation.aggregator.Max;
import cascading.pipe.Each;
import cascading.pipe.Every;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.scheme.Scheme;
import cascading.scheme.TextDelimited;
import cascading.scheme.TextLine;
import cascading.tap.Hfs;
import cascading.tap.Lfs;
import cascading.tap.Tap;
import cascading.tuple.Fields;

/**
 *
 * @author bradfordstephens
 */
public class Main {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {
        StockSymbolMax();
    }


    public static void StockSymbolMax()
    {

        //import data

  //      String source = "s3n://hadoopday/cascading/stockdata/small";
  //      String destination = "s3n://hadoopday/cascading/output/stockcountA/YOURNAME";


        String source = "/Users/bradfordstephens/Interop/Projects/Cascading-Tutorial/data";
        String destination = "/Users/bradfordstephens/Interop/Projects/Cascading-Tutorial/testout";

        //Input scheme for stock file -- this is a CSV
        Scheme sourceScheme = new TextDelimited(new Fields( "stock_exchange", "stock_symbol", "date", "stock_price_open",
                "stock_price_high", "stock_price_lo", "stock_price_close",
                 "stock_price_adj_close", "stock_volume"), true, ",");

        //Tap tap = new Hfs(sourceScheme, source);
        Tap tap = new Lfs(sourceScheme, source);
        Pipe pipe = new Pipe("Simple_Yahoo_Stocks");


        //cast fields to types... don't worry too much about this
        Class[] types = new Class[]{String.class, String.class, String.class, float.class,
        float.class, float.class, float.class,int.class, float.class };
        pipe = new Each(pipe, Fields.ALL, new Identity(types), Fields.REPLACE);

        //Calculate the highest stock_price_close for each symbol


        //Group by stock_symbol. Needs a unique name.
        //parameters: group name, input pipe, fields to group on
        pipe = new GroupBy("GroupSymbol",pipe, new Fields("stock_symbol"));


        //find max stock_price_close for each group
        //parameters: input pipe, input field, operation(new fields), output fields
        pipe = new Every(pipe,new Fields("stock_price_high"), new Max(new Fields("max_stock_price")), new Fields("stock_symbol", "max_stock_price"));

        //output schema
        Scheme destinationScheme = new TextDelimited(new Fields("stock_symbol", "max_stock_price"), ",");

        //output -- replace something if it already exists
        //Tap destTap = new Hfs(destinationScheme, destination, true);
        Tap destTap = new Lfs(destinationScheme, destination, true);


        FlowConnector connector = new FlowConnector();
        Flow flow = connector.connect(tap, destTap, pipe);
        
        
        //run code
        flow.complete();


    }


    //Exercise 1: For each day, find the highest closing price and associated symbol that day

    public static void Exercise1()
    {



        String source = "/Users/bradfordstephens/Interop/Projects/Cascading-Tutorial/data";
        String destination = "/Users/bradfordstephens/Interop/Projects/Cascading-Tutorial/testout";

        //Input scheme for stock file -- this is a CSV
        Scheme sourceScheme = new TextDelimited(new Fields( "stock_exchange", "stock_symbol", "date", "stock_price_open",
                "stock_price_high", "stock_price_lo", "stock_price_close",
                 "stock_price_adj_close", "stock_volume"), true, ",");

        //Tap tap = new Hfs(sourceScheme, source);
        Tap tap = new Lfs(sourceScheme, source);
        Pipe pipe = new Pipe("Simple_Yahoo_Stocks");


        //cast fields to types
        Class[] types = new Class[]{String.class, String.class, String.class, float.class,
        float.class, float.class, float.class,int.class, float.class };
        pipe = new Each(pipe, Fields.ALL, new Identity(types), Fields.REPLACE);


        /**
         * 
         * 
         * YOUR CODE HERE
         * 
         * 
         */

                //output schema
        Scheme destinationScheme = new TextDelimited(new Fields("date","stock_symbol", "max_stock_price"), ",");

        //output -- replace something if it already exists
        //Tap destTap = new Hfs(destinationScheme, destination, true);
        Tap destTap = new Lfs(destinationScheme, destination, true);


        FlowConnector connector = new FlowConnector();
        Flow flow = connector.connect(tap, destTap, pipe);


        //run code
        flow.complete();


    }



}
