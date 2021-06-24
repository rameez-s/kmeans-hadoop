import java.util.*;
import java.net.*;
import java.io.*;
import org.apache.hadoop.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.mapred.*;

public class DNA {

  public static String basePath = "hdfs://rameezs-n01.qatar.cmu.edu:9000/user/hadoop/";
  public static String baseHadoopPath = "hdfs://rameezs-n01.qatar.cmu.edu:9000/";

  public static void spawnJob(String inputPath, String outputPath, int numIter) throws Exception {
    JobConf job = new JobConf(DNA.class);
    job.setJobName("DNA " + numIter);

    job.setMapperClass(Map.class);
    job.setReducerClass(Reduce.class);

    job.setInputFormat(TextInputFormat.class);
    job.setOutputFormat(TextOutputFormat.class);

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);

    job.setNumReduceTasks(1);

    FileInputFormat.setInputPaths(job, new Path(inputPath));
    if (numIter == -1) {
      FileOutputFormat.setOutputPath(job, new Path(outputPath));
    }
    else {
      FileOutputFormat.setOutputPath(job, new Path(outputPath + numIter));
    }

    JobClient.runJob(job);
  }

  // Read current iteration from shared metadata file
	public static int readIter() {
		try {
    	Path iterPath = new Path(basePath + "metadata/iter.txt");
      FileSystem file = FileSystem.get(new Configuration());
      BufferedReader reader = new BufferedReader(new InputStreamReader(file.open(iterPath)));
      String line = reader.readLine();

      if(line != null) {
				return Integer.parseInt(line);
			}

      reader.close();
    } catch (Exception e){
				e.printStackTrace();
		}
    return 0;
	}

  public static int dnaDistance(String strand1, String strand2) {
    int dist = 0;
    for(int i = 0; i < strand1.length(); i++) {
      if(strand1.charAt(i) != strand2.charAt(i)) {
        dist++;
      }
    }
    return dist;
  }


	public static String maxBaseDNAGen(ArrayList<String> list) {
		String result = "";
    String bases = "ATGC";
		int[] baseFreqs = new int[] {0,0,0,0};
    int max = 0;
    int maxI = 0;

		for(int i = 0; i < list.get(0).length(); i++) {
			for(String strand : list) {
        switch(strand.charAt(i)) {
          case 'A':
            baseFreqs[0]++;
            break;
          case 'T':
            baseFreqs[1]++;
            break;
          case 'G':
            baseFreqs[2]++;
            break;
          case 'C':
            baseFreqs[3]++;
            break;
          default:
            break;
        }
			}

      max = 0;
      maxI = 0;
      for(int j = 0; j < 4; j++) {
        if(baseFreqs[j] > max) {
          maxI = j;
          max = baseFreqs[j];
        }
      }

			result += bases.charAt(maxI);
		}
		return result;
	}

	public static class Map extends MapReduceBase
                          implements Mapper<LongWritable, Text, Text, Text> {

		public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter report) throws IOException {
			Scanner scanner = new Scanner(value.toString());
      String line;
      String token;
      FileSystem file = FileSystem.get(new Configuration());

      Path centroidPath;
      BufferedReader reader;
      int currentIter;
      String[] rawIterData;
      int curDistance;
      int centIndex;
      String centDNA;

			while(scanner.hasNext()) {
				token = scanner.next();

				int minDist = Integer.MAX_VALUE;
				int minCentIndex = 0;
				try {
          centroidPath = new Path(basePath + "metadata/centroid.txt");
          reader = new BufferedReader(new InputStreamReader(file.open(centroidPath)));

          line = reader.readLine();
          currentIter = readIter();

          while (line != null) {
            rawIterData = line.split(",");
            // rawIterData: [iteration count, centroid index, dna]
            centIndex = Integer.parseInt(rawIterData[1]);
            centDNA = rawIterData[2];

            if(currentIter == Integer.parseInt(rawIterData[0])) {
            	curDistance = dnaDistance(token,centDNA);
            	if(curDistance < minDist) {
                minDist = curDistance;
                minCentIndex = centIndex;
              }
            }

            line = reader.readLine();
          }
          reader.close();
        } catch (Exception e) {
          e.printStackTrace();
        }

				output.collect(new Text(minCentIndex + ""), new Text(token));
			}
		}
	}

	public static class Reduce extends MapReduceBase implements Reducer<Text, Text, Text, Text> {

		public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter report) throws IOException{

			int count = 0;
			ArrayList<String> list = new ArrayList<String>();
      int numIter = 0;
      String centroidBuffer = "";
      String line = "";

      Path centroidPath;
      FileSystem file;
      BufferedReader reader;
      BufferedWriter writer;

			try {
        // Read old centroid vals
        centroidPath = new Path(basePath + "metadata/centroid.txt");
				numIter = readIter() + 1;

        file = FileSystem.get(new Configuration());
        reader = new BufferedReader(new InputStreamReader(file.open(centroidPath)));

				// construct buffer of old centroid vals
				line = reader.readLine();
        if (line != null) {
         	centroidBuffer = line;
         	line = reader.readLine();
         }

        while (line != null) {
        	centroidBuffer = centroidBuffer + "\n" + line;
          line = reader.readLine();
        }

        reader.close();
	      writer = new BufferedWriter(new OutputStreamWriter(file.create(centroidPath,true)));

				// collect all strands of each cluster
				while(values.hasNext()) {
					list.add((values.next()).toString());
					count++;
				}

				// Write updated average centroid vals and count
        line = maxBaseDNAGen(list);
        writer.write(centroidBuffer + "\n" + numIter + "," + key.toString() + "," + line);
        writer.close();
      } catch (Exception e) {
        e.printStackTrace();
      }

			output.collect(key, new Text(line + "," + count));
		}
	}

	public static void main(String args[]) throws Exception {
    String inputPath = args[0];
    String outputPath = args[1];
    int numPoints = Integer.parseInt(args[2]);
    int numCluster = Integer.parseInt(args[3]);
    int maxIter = Integer.parseInt(args[4]);
		int dnaLen = Integer.parseInt(args[5]);
    int numIter = 0;

    Path inPath;
    FileSystem file = FileSystem.get(new Configuration());
    BufferedReader inputReader;
    BufferedWriter centroidBuffer;
    String line;
    Path centroidPath;

    BufferedWriter iterWriter;

		try {
			inPath = new Path(baseHadoopPath + inputPath + "/input.txt");
      inputReader = new BufferedReader(new InputStreamReader(file.open(inPath)));

      line = inputReader.readLine();
      centroidPath = new Path(basePath + "metadata/centroid.txt");
      centroidBuffer = new BufferedWriter(new OutputStreamWriter(file.create(centroidPath,true)));

      while (line != null && numCluster > 0) {
        centroidBuffer.write("0," + numCluster + "," + line + "\n");
        numCluster--;

        line = inputReader.readLine();
      }
      inputReader.close();
      centroidBuffer.close();
    } catch (Exception e) {
      e.printStackTrace();
    }

		while (numIter < maxIter || (maxIter == 0 && numIter < 3)) {
			try {
	      inPath = new Path(basePath + "metadata/iter.txt");
	      iterWriter = new BufferedWriter(new OutputStreamWriter(file.create(inPath,true)));
	      iterWriter.write(numIter + "");
	      iterWriter.close();
      } catch (Exception e) {
        System.out.println("Could not write iter.txt");
      }

      if ((numIter+1 < maxIter || (maxIter == 0 && numIter+1 < 3))) {
        spawnJob(args[0], args[1], numIter);
      }
      else {
        spawnJob(args[0], args[1], -1);
      }
			numIter++;
		}
	}
}
