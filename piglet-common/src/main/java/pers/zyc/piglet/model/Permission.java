package pers.zyc.piglet.model;

/**
 * @author zhangyancheng
 */
public enum Permission {
	
	FULL,
	
	WRITE,
	
	READ,
	
	NONE;
	
	public Permission removeWrite() {
		return this == FULL || this == READ ? READ : NONE;
	}
	
	public Permission addWrite() {
		return this == FULL || this == READ ? FULL : WRITE;
	}
	
	public Permission removeRead() {
		return this == FULL || this == WRITE ? WRITE : NONE;
	}
	
	public Permission addRead() {
		return this == FULL || this == WRITE ? FULL : READ;
	}
	
	public boolean contains(Permission permission) {
		switch (this) {
			case FULL:
				return true;
			case WRITE:
				return permission == WRITE || permission == NONE;
			case READ:
				return permission == READ || permission == NONE;
			case NONE:
			default:
				return false;
		}
	}
	
	public boolean isWritable() {
		return contains(WRITE);
	}
	
	public boolean isReadable() {
		return contains(READ);
	}
}
