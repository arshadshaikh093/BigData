package org.programming.exercise

import util.control.Breaks._

object FractalSubString {
  
 def main(args: Array[String]) {
   //val inputString = "aaababcdbabdbcabdbcabdabcbadbacbcd"
   //val inputString = "aaaaaabbbbbcccccbcbcbcbcbcbbbbbcccd"
   /*val inputString = "aabcce"
   val legthOFSubString = shortestSubstring(inputString)
   println(legthOFSubString)*/
   
   
   val word = "abbcccb"
   //val word = "bbaacc"
   val k = 3
   
   compressWord(word, k)
   
 }
 
 
 def compressWord(word: String, K: Int): String = {
   val disticntChars = word.toCharArray().distinct
   val distinctCharSize = disticntChars.size
   println(distinctCharSize)
   var newWord = word
   val stringArray = Array.ofDim[String](distinctCharSize)
   println(stringArray.size)
   for(i <- 0 until distinctCharSize) {
     stringArray(i) = disticntChars(i).toString*K
   }
   
   for(k <- 0 until distinctCharSize){
       for(i <- 0 until distinctCharSize) {
         breakable{
         if(newWord.contains(stringArray(i))){
           println("inside")
           newWord = newWord.replaceAllLiterally(stringArray(i), "")
           println(newWord)
         }
         else {break}
         }
     }
   }
   
   
    return null

    }
 
 def shortestSubstring(inputString: String): Integer = {
   val disticntChars = inputString.toCharArray().distinct
   
   val size = inputString.size
   var subStringSize = disticntChars.size
   val distinctSubStringSize = subStringSize
   //val loopSize = size //- subStringSize
   
  while(subStringSize <= size) {
   for(i<-0 to size) {
     breakable {
         if(i+subStringSize <= size) {
           val subStringVal = inputString.substring(i, i+subStringSize)
           println(subStringVal)
           if(distinctSubStringSize == subStringVal.toCharArray().distinct.size) {
            println("Matched Substring " + subStringVal)
             return subStringVal.size
           }
         }
         else {
           break
           println("blanks")
         }
     }
   }
   subStringSize = subStringSize + 1
  }
    return 0
   
 }
 
 
 
 
}