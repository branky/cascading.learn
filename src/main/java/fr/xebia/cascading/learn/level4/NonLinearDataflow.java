package fr.xebia.cascading.learn.level4;

import cascading.flow.FlowDef;
import cascading.operation.aggregator.Count;
import cascading.operation.expression.ExpressionFilter;
import cascading.pipe.*;
import cascading.pipe.assembly.Discard;
import cascading.pipe.assembly.Rename;
import cascading.pipe.assembly.Retain;
import cascading.pipe.joiner.InnerJoin;
import cascading.tap.Tap;
import cascading.tap.hadoop.TemplateTap;
import cascading.tuple.Fields;

/**
 * Up to now, operations were stacked one after the other. But the dataflow can
 * be non linear, with multiples sources, multiples sinks, forks and merges.
 */
public class NonLinearDataflow {
	
	/**
	 * Use {@link CoGroup} in order to know the party of each presidents.
	 * You will need to create (and bind) one Pipe per source.
	 * You might need to correct the schema in order to match the expected results.
	 * 
	 * presidentsSource field(s) : "year","president"
	 * partiesSource field(s) : "year","party"
	 * sink field(s) : "president","party"
	 * 
	 * @see http://docs.cascading.org/cascading/2.1/userguide/html/ch03s03.html#N20650
	 */
	public static FlowDef cogroup(Tap<?, ?, ?> presidentsSource, Tap<?, ?, ?> partiesSource,
			Tap<?, ?, ?> sink) {
        Pipe presidentPipe = new Pipe("president");
        presidentPipe = new Rename(presidentPipe, new Fields("year"), new Fields("pre_year"));

        Pipe partyPipe = new Pipe("party");

        Pipe pipe = new CoGroup(new Pipe[] {presidentPipe, partyPipe}, new Fields[]{new Fields("pre_year"), new Fields("year")});
        pipe = new Retain(pipe, new Fields("president", "party"));

        return FlowDef.flowDef()//
                .addSource(presidentPipe, presidentsSource)
                .addSource(partyPipe, partiesSource) //
                .addTail(pipe)//
                .addSink(pipe, sink);
	}
	
	/**
	 * Split the input in order use a different sink for each party. There is no
	 * specific operator for that, use the same Pipe instance as the parent.
	 * You will need to create (and bind) one named Pipe per sink.
	 * 
	 * source field(s) : "president","party"
	 * gaullistSink field(s) : "president","party"
	 * republicanSink field(s) : "president","party"
	 * socialistSink field(s) : "president","party"
	 * 
	 * In a different context, one could use {@link TemplateTap} in order to arrive to a similar results.
	 * @see http://docs.cascading.org/cascading/2.1/userguide/htmlsingle/#N214FF
	 */
	public static FlowDef split(Tap<?, ?, ?> source,
			Tap<?, ?, ?> gaullistSink, Tap<?, ?, ?> republicanSink, Tap<?, ?, ?> socialistSink) {
        Pipe pipe = new Pipe("input");

        Pipe gaullistPipe = new Pipe("gaullist", pipe);
        gaullistPipe = new Each(gaullistPipe, new Fields("party"), new ExpressionFilter("!party.equals(\"Gaullist\")", String.class));


        Pipe republicanPipe = new Pipe("republican", pipe);
        republicanPipe = new Each(republicanPipe, new Fields("party"), new ExpressionFilter("!party.equals(\"Republican\")", String.class));

        Pipe socialistPipe = new Pipe("socialist", pipe);
        socialistPipe = new Each(socialistPipe, new Fields("party"), new ExpressionFilter("!party.equals(\"Socialist\")", String.class));


        return FlowDef.flowDef()//
                .addSource(pipe, source)
                .addTail(gaullistPipe)//
                .addTail(republicanPipe)//
                .addTail(socialistPipe)//
                .addSink(gaullistPipe, gaullistSink)
                .addSink(republicanPipe, republicanSink)
                .addSink(socialistPipe, socialistSink);
	}


    public static FlowDef split(Tap<?, ?, ?> source,
                                Tap<?, ?, ?> templateSink) {
        Pipe pipe = new Pipe("input");

        Pipe gaullistPipe = new Pipe("gaullist", pipe);
        gaullistPipe = new Each(gaullistPipe, new Fields("party"), new ExpressionFilter("!party.equals(\"Gaullist\")", String.class));


        Pipe republicanPipe = new Pipe("republican", pipe);
        republicanPipe = new Each(republicanPipe, new Fields("party"), new ExpressionFilter("!party.equals(\"Republican\")", String.class));

        Pipe socialistPipe = new Pipe("socialist", pipe);
        socialistPipe = new Each(socialistPipe, new Fields("party"), new ExpressionFilter("!party.equals(\"Socialist\")", String.class));


        return FlowDef.flowDef()//
                .addSource(pipe, source)
                .addTail(pipe)//
                .addSink(pipe, templateSink);
    }
	
}
