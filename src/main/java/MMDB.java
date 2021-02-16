
public abstract class MMDB {

    abstract Object Connection();
	
    /*
     * Query 1. For a given customer, find his/her all related data including profile, 
     * orders, invoices, feedback, comments, and posts in the last month, 
     * return the category in which he/she has bought the largest number of products, 
     * and return the tag which he/she has engaged the greatest times in the posts.
     * */
    abstract void Q1(String PersonId);
    
    /*
     * Query 2. For a given product during a given period, find the people who commented 
     * or posted on it, and had bought it.
     * */
    abstract void Q2(String ProductId); 
    
    /*
     * Query 3. For a given product during a given period, find people who have undertaken 
     * activities related to it, e.g., posts, comments, and review, 
     */
    abstract void Q3(String ProductId); 
    
    /*
     * Query 4. Find the top-2 persons who spend the highest amount of money in orders. 
     * Then for each person, traverse her knows-graph with 3-hop to find the friends, 
     * and finally return the common friends of these two persons.*/
    abstract void Q4(); 
    
    /*
     * Query 5. Given a start customer and a product category, find persons who are this 
     * customer's friends within 3-hop friendships in Knows graph, besides, they have bought 
     * products in the given category. Finally, return feedback with the 5-rating review of 
     * those bought products.
     * */
    abstract void Q5(String PersonId, String brand); 
    
    /*
     * Query 6. Given customer 1 and customer 2, find persons in the shortest path between them 
     * in the subgraph, and return the TOP 3 best sellers from all these persons' purchases.*/
    abstract void Q6(String startPerson, String EndPerson); 
    
    /*
     * Query 7. For the products of a given vendor with declining sales compare to the former quarter, 
     * analyze the reviews for these items to see if there are any negative sentiments.*/
    abstract void Q7(String brand); 
    
    /*
     * Query 8. For all the products of a given category during a given year, compute its total sales amount, 
     * and measure its popularity in the social media.
     * */
    abstract void Q8(); 
    
    /*
     * Query 9. Find top-3 companies who have the largest amount of sales at one country, for each company, 
     * compare the number of the male and female customers, and return the most recent posts of them.
     * */
    abstract void Q9();
    
    /*
     * Query 10. Find the top-10 most active persons by aggregating the posts during the last year, then calculate 
     * their RFM (Recency, Frequency, Monetary) value in the same period, and return their recent reviews and tags 
     * of interest.
     * */
    abstract void Q10();

    /*
     * Q11 (Relational+RDF)
     * For all the products, find all the related resources, predicates,
     * and objects in the RDF graph. The predicate contains a prefix of "http://dbpedia.org/property/"
     * */
    abstract void Q11();

    /*
     * Query 12 (Relational+JSON+RDF)
     * For all the related resources with a type property in the RDF graph,
     * return all the related orders with a given date
     * */
    abstract void Q12();

    /*
     * Query 13 (Relation+RDF+Property Graph)
     * Given a brand, return all the products in the table,
     * all the related resources, predicates, and objects in the RDF graph,
     * and all the related posts in the property graph.
     * */
    abstract void Q13();

    /*
     * Query 14. (Aggregation1) (Relation+RDF+JSON)
     * Given a brand, return TOP-20 products in the product table that have the largest number of the related resources in the RDF graph,
     * then count and sort the related orders for each person with a given date.
     * */
    abstract void Q14();

    /*
     * Query 15. (Aggregation2) (Relation+RDF+Property Graph)
     * Given a brand, return TOP-20 products in the table that have the largest number of the related resources in the RDF graph,
     * then count  and sort the related posts for each tag of product in the property graph.
     *
     * */
    abstract void Q15();
}
