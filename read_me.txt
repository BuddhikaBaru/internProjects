1.1 hypothesis test_1
	select one configuration and one PM column at a time. then loop for every configuration and PM.
	Notes - 
	change the file name and path when run the code for dataset.vdata should be in csv file. double check 
	for the columns that are droped manually. (ID, Dates and other unnecessory columns.)

1.2 Hypothesis test_2 Pairs of CFGs
	select two of CFGs at once. then get STD,Mean,Count for all PM considering total sample and combination pairs
	(unique settings in each CFG). calculate Z score and Probebility value. 
	
	Note - 
		Use these folder structure for the Programe. code and input file should be in same folder.(you can 		change path manually)
		Prgrame is run as two stages.

2.1 Cell Upgrade Peformance check

	these is the programe for compare perfomance before and after cell upgrade(split or massive MIMO). 
	
	Note - 
		Use the same folder structure. API is also included. 
		API calling process is consider the upgrade date and the sites that upgraded in that day. 

3.1 Graph for Azimuth finding Project

	Note - 
		3 types of graphs are drawn by this program.
		1. user azimuth vs distance based on connected cell.
		2. mean of RSRP and RSRQ in grid squers in user azimuth vs distance graph. Two graphs (both RSRP and 			RSRQ) ploted in same axis.
		3. above graphs in two subplots. Gradiant applied to represent the strength of each signal


4.0 Basestation Troughput Prediction 

4.1.1 RFC model Code
	RFC model train, test, save as SAV file, load and predict using it.
	Accuracy of the model is depend on the data set given and tuning the hyper parameters.
	A grid is used for tune the model. the grid can be change for train another model as necessary. 
	When the sample data set choosing, consider about repeating same king of rows, variance of classes,
	number of samples for each class types etc.

4.1.2 RFC_Model

	A trained model for predicting throughput. accuracy arround 9.5

4.2 Linear Regression

	Linear regression and coefficiants for prediction formula

4.3 Decission Tree for Prediction


