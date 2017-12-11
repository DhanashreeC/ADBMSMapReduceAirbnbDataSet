package org.adbms.finalproject.listingandreviewshierarchy;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.xml.sax.SAXException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.*;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import java.io.IOException;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.List;

public class ListingReviewReducer extends Reducer<Text,Text,Text,NullWritable>
	{

	private ArrayList<String> reviews=new ArrayList<String>();
	private DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
	private String listing = null;

	@Override
	public void reduce(Text key, Iterable<Text> value, Context context) throws IOException, InterruptedException {
		listing = null;
		reviews.clear();

		for (Text text : value) {
			if (text.charAt(0) == 'L') {
				listing = text.toString().substring(1, text.toString().length()).trim();
			} else {
				reviews.add(text.toString().substring(1, text.toString().length()).trim());
			}
		}

		if (listing != null) {

			try {
				String moviesWithTags = nestedElements(listing, reviews);
				context.write(new Text(moviesWithTags), NullWritable.get());

			} catch (ParserConfigurationException e) {
				e.printStackTrace();
			} catch (SAXException e) {
				e.printStackTrace();
			} catch (TransformerException e) {
				e.printStackTrace();
			}

		}
	}

	private String nestedElements(String listingid, List<String> reviews)
			throws ParserConfigurationException, IOException, SAXException, TransformerException {
		DocumentBuilder db = dbf.newDocumentBuilder();
		Document doc = db.newDocument();

		Element rootElement = doc.createElement("listing");
		rootElement.setAttribute("listingid", listingid);
		doc.appendChild(rootElement);

		for (String tagsXml : reviews) {
			Element tag = doc.createElement("review");
			tag.appendChild(doc.createTextNode(tagsXml));
			rootElement.appendChild(tag);

		}
		return transforDocumentToString(doc);
	}

	private String transforDocumentToString(Document doc) throws TransformerException {
		TransformerFactory tf = TransformerFactory.newInstance();
		Transformer transformer = tf.newTransformer();
		transformer.setOutputProperty(OutputKeys.OMIT_XML_DECLARATION, "yes");

		StringWriter writer = new StringWriter();
		transformer.transform(new DOMSource(doc), new StreamResult(writer));

		return writer.getBuffer().toString().replaceAll("\n|\r", "");
	}

}
