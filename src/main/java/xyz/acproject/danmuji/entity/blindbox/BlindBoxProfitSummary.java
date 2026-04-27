package xyz.acproject.danmuji.entity.blindbox;

import lombok.Data;

@Data
public class BlindBoxProfitSummary {
    private Long roomId;
    private Long uid;
    private Long fromTimestamp;
    private Long toTimestamp;
    private Long totalCostCoin = 0L;
    private Long totalRewardCoin = 0L;
    private Long totalProfitCoin = 0L;
    private Long totalCount = 0L;
}

