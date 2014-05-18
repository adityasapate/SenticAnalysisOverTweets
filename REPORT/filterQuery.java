FilterQuery tweetFilterQuery = new FilterQuery(); 
tweetFilterQuery.track(new String[]{"BJP"});
tweetFilterQuery.language(new String[]{"en"});
ConfigurationBuilder configurationBuilder = new ConfigurationBuilder();
        configurationBuilder.setOAuthConsumerKey(_consumerKey)
                .setOAuthConsumerSecret(_consumerSecret)
                .setOAuthAccessToken(_accessToken)
                .setOAuthAccessTokenSecret(_accessTokenSecret);
tweetFilterQuery.locations(new double[][]{
	new double[]{-126.56,30.44},
                new double[]{-61.17,44.08
                }});
