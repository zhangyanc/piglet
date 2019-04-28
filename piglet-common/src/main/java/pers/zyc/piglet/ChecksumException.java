package pers.zyc.piglet;

/**
 * @author zhangyancheng
 */
public class ChecksumException extends SystemException {

	public ChecksumException() {
		super(SystemCode.CHECKSUM_WRONG);
	}
}
