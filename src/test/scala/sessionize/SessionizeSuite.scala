package sessionize

import org.scalatest.FunSuite
import java.text.SimpleDateFormat
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner


import Sessionize._

@RunWith(classOf[JUnitRunner])
class SessionizeSuite extends FunSuite  {

  test("sessionize: parse time") {
    val line = "2015-07-22T09:00:28.019143Z marketpalce-shop 123.242.248.130:54635 10.0.6.158:80 0.000022 0.026109 0.00002 200 200 0 699 \"GET https://paytm.com:443/shop/authresponse?code=f2405b05-e2ee-4b0d-8f6a-9fed0fcfe2e0&state=null HTTP/1.1\" \"Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/43.0.2357.130 Safari/537.36\" ECDHE-RSA-AES128-GCM-SHA256 TLSv1.2"
    val dateformat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
    assert(parseTime(line, dateformat) === dateformat.parse("2015-07-22T09:00:28.019Z"))
  }
  
  test("sessionize: validate valid request") {
    val line = "2015-07-22T09:00:28.019143Z marketpalce-shop 123.242.248.130:54635 10.0.6.158:80 0.000022 0.026109 0.00002 200 200 0 699 \"GET https://paytm.com:443/shop/authresponse?code=f2405b05-e2ee-4b0d-8f6a-9fed0fcfe2e0&state=null HTTP/1.1\" \"Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/43.0.2357.130 Safari/537.36\" ECDHE-RSA-AES128-GCM-SHA256 TLSv1.2"
    assert(validRequest(line) === true)
  }
  
  test("sessionize: validate invalid request - bad status") {
    val line = "2015-07-22T09:00:28.019143Z marketpalce-shop 123.242.248.130:54635 10.0.6.158:80 0.000022 0.026109 0.00002 404 404 0 699 \"GET https://paytm.com:443/shop/authresponse?code=f2405b05-e2ee-4b0d-8f6a-9fed0fcfe2e0&state=null HTTP/1.1\" \"Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/43.0.2357.130 Safari/537.36\" ECDHE-RSA-AES128-GCM-SHA256 TLSv1.2"
    assert(validRequest(line) === false)
  }
  
  test("sessionize: validate invalid request - not GET method") {
    val line = "2015-07-22T09:00:28.019143Z marketpalce-shop 123.242.248.130:54635 10.0.6.158:80 0.000022 0.026109 0.00002 200 200 0 699 \"POST https://paytm.com:443/shop/authresponse?code=f2405b05-e2ee-4b0d-8f6a-9fed0fcfe2e0&state=null HTTP/1.1\" \"Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/43.0.2357.130 Safari/537.36\" ECDHE-RSA-AES128-GCM-SHA256 TLSv1.2"
    assert(validRequest(line) === false)
  }
  
  test("sessionize: in window - in") {
    val line = "2015-07-22T09:01:28.019143Z marketpalce-shop 123.242.248.130:54635 10.0.6.158:80 0.000022 0.026109 0.00002 200 200 0 699 \"POST https://paytm.com:443/shop/authresponse?code=f2405b05-e2ee-4b0d-8f6a-9fed0fcfe2e0&state=null HTTP/1.1\" \"Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/43.0.2357.130 Safari/537.36\" ECDHE-RSA-AES128-GCM-SHA256 TLSv1.2"
    val dateformat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
    val start = "2015-07-22T09:00:00.000Z"
    val end = "2015-07-22T09:15:00.000Z"
    
    assert(inWindow(line, dateformat.parse(start), dateformat.parse(end)) === true)
  }
  
  test("sessionize: in window - out") {
    val line = "2015-07-22T09:28:28.019143Z marketpalce-shop 123.242.248.130:54635 10.0.6.158:80 0.000022 0.026109 0.00002 200 200 0 699 \"POST https://paytm.com:443/shop/authresponse?code=f2405b05-e2ee-4b0d-8f6a-9fed0fcfe2e0&state=null HTTP/1.1\" \"Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/43.0.2357.130 Safari/537.36\" ECDHE-RSA-AES128-GCM-SHA256 TLSv1.2"
    val dateformat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
    val start = "2015-07-22T09:00:00.000Z"
    val end = "2015-07-22T09:15:00.000Z"
    
    assert(inWindow(line, dateformat.parse(start), dateformat.parse(end)) === false)
  }
  
  
}
