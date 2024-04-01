# kabini
**Selectivity Estimation for Range Predicates using
Lightweight Models**

Selectivity
estimation for conjunctive range queries on a dataset with
numerical attributes. The problem was approached by developing a regression model, aimed at estimating the selectivity
(i.e., the actual number of rows in the dataset satisfying the
query predicates) for such queries. To facilitate the learning
process, we constructed a vectorization method that produces
the ranges of the query and its actual selectivity value. The
objective was to train the regression model to produce estimated selectivity values (est(q)) that closely matched the actual
selectivity values (act(q)) for any conjunctive range query on
the dataset. We expected the trained model to provide accurate
estimates for queries that were well-represented in the training
set. This problem formulation guided our implementation of
the regression model from the initial paper and subsequent
implementation of an additional lightweight model that would
help us in comparing all the three regression methods used.
