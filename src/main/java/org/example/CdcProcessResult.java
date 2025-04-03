package org.example;

import lombok.*;

import java.io.Serializable;
import java.util.List;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@ToString
public class CdcProcessResult implements Serializable {
    private CdcTable currentCdc;
    private List<FactorMeta> factor;
}