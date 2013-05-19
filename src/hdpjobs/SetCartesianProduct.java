package hdpjobs;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * A simple hadoop mapreduce job that generates the cartesian product of two datasets R and S to 
 * generate all the entity pairs, applies a series of negative rules (This is equivalent to a blocking
 * step. A negative rule is a rule which determine whether two entities are dissimilar. It can be viewed
 * as the branch of a decision tree that ends in a negative classification while comparing two entities) 
 * and stages all the entity pairs that survive these rules. These are the entity pairs that are worthy of 
 * further complex entity matching.
 * 
 * @author excelsior
 *
 */
public class SetCartesianProduct extends Configured implements Tool 
{

	public int run(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
		long startTime = System.currentTimeMillis();

		Configuration conf = new Configuration();

	    Job job = new Job(conf, "Entity Matcher Job");
	    job.setJarByClass(SetCartesianProduct.class);

	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    
	    job.setMapperClass(EntityMatchMapper.class);
	    job.setReducerClass(EntityMatchReducer.class);

	    job.setMapOutputKeyClass(Text.class);
	    job.setMapOutputValueClass(Citation.class);

	    job.setOutputKeyClass(NullWritable.class);
	    job.setOutputValueClass(Text.class);
	    
	    boolean isJobComplete = job.waitForCompletion(true);
	    long endTime = System.currentTimeMillis();
	    int jobRunTime = (int)(endTime - startTime)/1000;
	    
	    System.out.println("Time taken for job : " + jobRunTime + " seconds .");
	    
	    System.exit(isJobComplete ? 0 : 1);
		return 1;
	}
	
	// -------------- Utility functions for mapper and reducer classes ---------------
	public static class Citation implements WritableComparable<Citation>
	{
		private Text 		id;
		private Text 		title;
		//private List<Text> 	authors; // TODO : How do I take care of a data structure serialisation ?
		private Text 		venue;
		private IntWritable year;

		public Citation()
		{
			this.id = new Text();
			this.title = new Text();
			this.venue = new Text();
			this.year = new IntWritable();
		}

		public Citation(String id, String title, List<String> authors, String venue, int year)
		{
			this.id = new Text(id);
			this.title = new Text(title);
//			this.authors = authors;
			this.venue = new Text(venue);
			this.year = new IntWritable(year);
		}

		public static Citation clone(Citation src)
		{
			return new Citation(src.id.toString(), src.title.toString(), null, src.venue.toString(), src.year.get());
		}

		@Override
		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			if (obj == null)
				return false;
			if (getClass() != obj.getClass())
				return false;
			Citation other = (Citation) obj;
			/*if (authors == null) {
				if (other.authors != null)
					return false;
			} else if (!authors.equals(other.authors))
				return false;*/
			if (id == null) {
				if (other.id != null)
					return false;
			} else if (!id.equals(other.id))
				return false;
			if (title == null) {
				if (other.title != null)
					return false;
			} else if (!title.equals(other.title))
				return false;
			if (venue == null) {
				if (other.venue != null)
					return false;
			} else if (!venue.equals(other.venue))
				return false;
			if (year != other.year)
				return false;
			return true;
		}


		@Override
		public String toString() {
			return "Citation [id=" + id + ", title=" + title + ", authors="
					+ ", venue=" + venue + ", year=" + year + "]";
		}

		public Text getId() {
			return id;
		}

		public void setId(Text id) {
			this.id = id;
		}

		public Text getTitle() {
			return title;
		}

		public void setTitle(Text title) {
			this.title = title;
		}

		public Text getVenue() {
			return venue;
		}

		public void setVenue(Text venue) {
			this.venue = venue;
		}

		public IntWritable getYear() {
			return year;
		}

		public void setYear(IntWritable year) {
			this.year = year;
		}

		@Override
		public void readFields(DataInput in) throws IOException {
			id.readFields(in);
			title.readFields(in);
			venue.readFields(in);
			year.readFields(in);
		}

		@Override
		public void write(DataOutput out) throws IOException {
			id.write(out);
			title.write(out);
			venue.write(out);
			year.write(out);
		}

		@Override
		public int compareTo(Citation that) {
			return this.id.compareTo(that.id);
		}

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + ((id == null) ? 0 : id.hashCode());
			result = prime * result + ((title == null) ? 0 : title.hashCode());
			result = prime * result + ((venue == null) ? 0 : venue.hashCode());
			result = prime * result + year.hashCode();
			return result;
		}

		public static List<String> getAuthors(String authorsRecord)
		{
			List<String> authors = new ArrayList<String>();	
			String[] authorStr = authorsRecord.trim().replace("\"", "").split(",");
			for(String author : authorStr) {
				authors.add(author.trim());
			}

			return authors;
		}

		/*
		 * Parse the text and convert to a Citation object for processing.
		 * TODO : Current implementation is hacky. Can it be improved ?
		 */
		public static Citation parseCitation(String citationRecord)
		{
			try {
				String[] splitRecord = citationRecord.split(",\"");
				
				String id = splitRecord[0].trim().replace("\"", "");
				String title = splitRecord[1].trim().replace("\"", "");
				
				String tempVenueYear = null;
				if(splitRecord.length == 3) {
					tempVenueYear = splitRecord[2].trim().replace("\"", "");
				}
				else {
					tempVenueYear = splitRecord[3].trim().replace("\"", "");
				}

				int venueYearSeparator = tempVenueYear.lastIndexOf(",");
				
				String venue = tempVenueYear.substring(0, venueYearSeparator).trim();
				int year = Integer.parseInt(tempVenueYear.substring(venueYearSeparator+1));
				
				//List<String> authors = getAuthors(splitRecord[2]);	
				return new Citation(id, title, null, venue, year);
			}
			catch(Exception e) {
				System.out.println("Failed to parse the input record : " + citationRecord +". Reason : " + e);
				return null;
			}
		}

	}

	/*
	 * Represents a pair of citation objects that are worthy candidates of complex entity matching.
	 */
	public static class CitationPair
	{
		private Citation entityA;
		private Citation entityB;
		
		public CitationPair(Citation entityA, Citation entityB)
		{
			this.entityA = entityA;
			this.entityB = entityB;
		}
		
		@Override
		public String toString()
		{
			return "{" + entityA.toString() + " | " + entityB.toString() + "}";
		}
	}
	
	/*
	 * Classifies a citation record as belonging to set R or S.
	 */
	public static boolean isEntityOfSetR(Citation entity)
	{
		String citationId = entity.id.toString();
		return citationId.matches("[0-9]*") ? true : false;
	}
	
	/*
	 * Applies a simple No-rule to check if two citations are same. For this experiment, the
	 * assumption is that if two citations don't have the same year then they cannot be 
	 * same entities and this pair can be immediately ignored for the rest of the entity matching
	 * process.
	 * 
	 * TODO : Add drools rules here.
	 */
	public static boolean areSameEntities(Citation entityA, Citation entityB)
	{
		boolean areSameEntities = true;
		
		// Test with a simple No-rule here
		if(!entityA.getYear().equals(entityB.getYear())) {
			areSameEntities = false;
		}

		return areSameEntities;
	}

	public static String extractReduceKey(String key)
	{
		String compKey = key.toString();
		compKey = compKey.substring(0, compKey.length() - 2).trim();

		return compKey;
	}
	
	// --------- Mapper class ---------------
	/*
	 * Let us suppose that the data set R and S get split into X and Y chunks. Thus, the mapper
	 * function emits Y keys for each entity in set R for each of the X chunks. The form of the
	 * key is <indexR. indexS. sourceOfData{R|S}>. The same process is repeated for a record in 
	 * the other data set.
	 */
	public static class EntityMatchMapper extends Mapper<Object, Text, Text, Citation> {
		// Number of blocks into which R file is splitted
		private static int X = 1;
		
		// Number of blocks into which S file is splitted
		private static int Y = 1;
		
		private String KEY_SEPARATOR = ".";
		
		@Override
		public void map(Object key, Text entityRecord, Context context) throws IOException, InterruptedException {
			Citation entity = Citation.parseCitation(entityRecord.toString());
			if(entity == null) {
				return;
			}
			
			String dataSource = null;
			if(isEntityOfSetR(entity)) {
				dataSource = "R";
			}
			else {
				dataSource = "S";
			}
			
			StringBuilder mapKey = new StringBuilder();
			mapKey.append(Integer.toString(1)).append(KEY_SEPARATOR);
			mapKey.append(Integer.toString(1)).append(KEY_SEPARATOR);
			mapKey.append(dataSource);
			
			context.write(new Text(mapKey.toString()), entity);
		}
	}
	
	// --------- Reducer class --------------
	public static class EntityMatchReducer extends Reducer<Text, Citation, NullWritable, Text> {
		private Map<String, List<Citation>> citationsSetR = null;
		
		protected void setup(Context context) throws IOException, InterruptedException {
			if(citationsSetR == null) {
				citationsSetR = new HashMap<String, List<Citation>>();	
			}
		}

		public void reduce(Text cartesianProductKey, Iterable<Citation> entities, Context context)  throws IOException, InterruptedException {
			for(Citation entity : entities) {
				String key = cartesianProductKey.toString();
				String reducedKey = extractReduceKey(key);
				if(key.contains(".R")) { // Set R objects have .R at the end of the key
					List<Citation> entries = citationsSetR.get(reducedKey);
					if(entries == null) {
						entries = new ArrayList<Citation>();
					}
					
					Citation entityClone = Citation.clone(entity);
					entries.add(entityClone);
					citationsSetR.put(reducedKey, entries);
				}
				else if(key.contains(".S")) { // Set S objects have .S at the end of the key
					 if(citationsSetR.containsKey(reducedKey)) {
						List<Citation> entitiesR = citationsSetR.get(reducedKey);
						for(Citation entityR : entitiesR) {
							boolean areSameEntities = areSameEntities(entityR, entity);
							if(areSameEntities) {
								CitationPair entityPair = new CitationPair(entityR, entity);
								context.write(null, new Text(entityPair.toString()));
							}
						}
					}
				}
				
			}
		}
	}

	public static void main(String[] args) throws Exception {
		System.out.println("Starting the Entity matching using cartesian product job ..");
		int exitCode = ToolRunner.run(new SetCartesianProduct(), args);
		System.exit(exitCode);
	}

}
