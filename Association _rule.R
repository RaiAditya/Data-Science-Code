# Reading file
item_to_id = read.csv("item_to_id.csv")
purchase_history = read.csv("purchase_history.csv")
pp = read.csv("mba_pb.csv", header = FALSE)
txnqq = read.transactions(file="mba_pb.csv", rm.duplicates= TRUE, format="basket", sep=",",cols=1)
summary(txnqq)
#loading plyr package after detaching dplyr
if(sessionInfo()['basePkgs']=="dplyr" | sessionInfo()['otherPkgs']=="dplyr"){
  detach(package:dplyr, unload=TRUE)
}
library(plyr)
#install.packages("arules")
library(arules)

 
#preparing the input for aprior model 
dt = purchase_history[ c(2) ] 
write.csv(dt,"ItemList.csv", row.names = TRUE , quote = FALSE)
txn = txnqq
#txn = read.transactions(file="ItemList.csv", rm.duplicates= TRUE, format="basket",sep=",",cols=1);

txn@itemInfo$labels <- gsub("\"","",txn@itemInfo$labels)
basket_rules <- apriori(txn,parameter = list(sup = 0.01, conf = 0.5,target="rules"));
if(sessionInfo()['basePkgs']=="tm" | sessionInfo()['otherPkgs']=="tm"){
  detach(package:tm, unload=TRUE)
}

inspect(basket_rules)

#Alternative to inspect() is to convert rules to a dataframe and then use View()
df_basket <- as(basket_rules,"data.frame")
View(df_basket)
library(dplyr)
df_basket = df_basket %>% arrange(desc(count)) 
top10_trans = df_basket %>% head(10)

# Converting the results in readable form
library(tidyr)
dff = separate(df_basket, rules , into = c("item", "which_bought_also"), sep = "=>")
ff$which_bought_also = gsub("\\{|\\]", "", as.character(ff$which_bought_also))
ff$which_bought_also = gsub("\\}|\\]", "", as.character(ff$which_bought_also))
ff$which_bought_also =as.numeric(as.character(ff$which_bought_also))

ff$item = gsub("\\{|\\]", "", as.character(ff$item))
ff$item = gsub("\\}|\\]", "", as.character(ff$item))
item_to_id$item = as.numeric(as.character(item_to_id$Item_id))
ffd = left_join(ff, item_to_id , by = c("which_bought_also" = "item"))

library(stringr)
ffd$item_bought1 = as.numeric(as.character(str_split_fixed(ffd$item, ",",2)[,1]))
ffd$item_bought2 = as.numeric(as.character(str_split_fixed(ffd$item, ",",2)[,2]))
ffd = left_join(ffd, item_to_id , by = c("item_bought1" = "Item_id"))
ffd = left_join(ffd, item_to_id , by = c("item_bought2" = "Item_id"))
colnames(ffd)
final = ffd[ c(3:7,11,13) ]
colnames(final)
colnames(final) <- c("Support","confidence" , "lift" ,"count" ,"item_also_bought" , "when_item_bought","when_item_bought")

write.csv(dff,'rules.csv',row.names=F)
View(final)
dfff = dff %>% mutate(sup_conf = support*confidence)
dfff= dfff %>% filter(sup_conf > 0.01)
video = c('175566','180317','174881','174890','179258','175561','174873','174868','174888','174869')
video = read.csv("video_list.csv")
x <- 1:119
for (i in x)
{
  one = dfff %>% filter(grepl(video$video[i], item))
  #str(oneff)
  if (length(one$item) > 0){
  one = one %>% arrange(desc(count))
  one$video = video$video[i]
  onef = one %>% group_by(video,which_bought_also) %>% summarise(mean_lift = mean(lift))
  oneff = data.frame(onef)
  oneff = oneff %>% arrange(desc(mean_lift)) %>% head(5)
  df = rbind(df,oneff)
  }
}
#str(df)
write.csv(df, "rules_pb3.csv")
df <- data.frame()

i = 1
video$video[2]


