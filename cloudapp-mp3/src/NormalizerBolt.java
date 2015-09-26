import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.util.Arrays;
import java.util.List;

/**
 * A bolt that normalizes the words, by removing common words and making them lower case.
 */
public class NormalizerBolt extends BaseBasicBolt {
  private List<String> commonWords = Arrays.asList("the", "be", "a", "an", "and",
      "of", "to", "in", "am", "is", "are", "at", "not", "that", "have", "i", "it",
      "for", "on", "with", "he", "she", "as", "you", "do", "this", "but", "his",
      "by", "from", "they", "we", "her", "or", "will", "my", "one", "all", "s", "if",
      "any", "our", "may", "your", "these", "d" , " ", "me" , "so" , "what" , "him" );

  @Override
  public void execute(Tuple tuple, BasicOutputCollector collector) {

    /*
    ----------------------TODO-----------------------
    Task:
     1. make the words all lower case
     2. remove the common words

    ------------------------------------------------- */
	String sentence = tuple.getString(0);
	sentence= sentence.trim().toLowerCase();
	
	String[]words=sentence.split(" ");
	for(String word:words){
		if (!commonWords.contains(word) && !word.equals(" ")){
			//String smallWord = word.toLowerCase();
			collector.emit(new Values(word));
		}
	}
}

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(new Fields("word"));
  }
}
