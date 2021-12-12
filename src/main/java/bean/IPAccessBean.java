package bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author mangguodong
 * @create 2021-12-12
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class IPAccessBean {
    private String clientIp;
    private String uri;
    private String requestUrl;
    private String httpRequestId;
    //private String uriType;
    //private String requestUrlType;
    private Long timestamp;
}
